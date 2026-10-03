from __future__ import annotations

import json
import os
import queue
import random
import shutil
import time
from collections import defaultdict
from dataclasses import dataclass, asdict
from pathlib import Path
from multiprocessing import Process, Queue
from typing import Any, Callable, Iterable, TypeVar

K1 = TypeVar("K1")
V1 = TypeVar("V1")
K2 = TypeVar("K2")
V2 = TypeVar("V2")
V3 = TypeVar("V3")

KeyValue = tuple[Any, Any]
Mapper = Callable[[int, str], Iterable[KeyValue]]
Reducer = Callable[[Any, Iterable[Any]], Iterable[KeyValue] | KeyValue]
Combiner = Callable[[Any, Iterable[Any]], Iterable[KeyValue] | KeyValue]


@dataclass
class MapReduceConfig:
    num_map_tasks: int = 4
    num_reduce_tasks: int = 2
    num_workers: int = 4
    use_combiner: bool = True
    work_dir: str = "mr_work"
    task_timeout: float = 20.0
    max_retries: int = 2
    simulate_worker_crash: bool = False
    seed: int = 42


@dataclass
class JobStats:
    input_records: int = 0
    map_tasks: int = 0
    reduce_tasks: int = 0
    retries: int = 0
    mapper_records: int = 0
    records_after_combiner: int = 0
    shuffle_records: int = 0
    shuffle_bytes: int = 0
    output_records: int = 0
    elapsed_seconds: float = 0.0

    def pretty(self) -> str:
        return "\n".join(
            [
                "=== JOB STATS ===",
                f"input records       : {self.input_records}",
                f"map tasks           : {self.map_tasks}",
                f"reduce tasks        : {self.reduce_tasks}",
                f"task retries        : {self.retries}",
                f"map output records  : {self.mapper_records}",
                f"after combiner      : {self.records_after_combiner}",
                f"shuffle records     : {self.shuffle_records}",
                f"shuffle bytes       : {self.shuffle_bytes:,}",
                f"output records      : {self.output_records}",
                f"elapsed             : {self.elapsed_seconds:.3f}s",
            ]
        )


def _normalize_output(result: Any) -> list[KeyValue]:
    if isinstance(result, tuple) and len(result) == 2:
        return [result]
    return list(result)


def _write_jsonl(path: Path, records: Iterable[KeyValue]) -> int:
    count = 0
    with path.open("w", encoding="utf-8") as f:
        for key, value in records:
            line = json.dumps([key, value], separators=(",", ":"))
            f.write(line + "\n")
            count += 1
    return count


def _read_jsonl(path: Path) -> Iterable[KeyValue]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            key, value = json.loads(line)
            yield key, value


def _partition(key: Any, num_reducers: int) -> int:
    # A deterministic partitioner is useful for repeatable experiments.
    encoded = json.dumps(key, sort_keys=True).encode("utf-8")
    h = 2166136261
    for b in encoded:
        h ^= b
        h = (h * 16777619) & 0xFFFFFFFF
    return h % num_reducers


def _map_worker(
    task_id: int,
    records: list[tuple[int, str]],
    num_reducers: int,
    work_dir: str,
    mapper: Mapper,
    combiner: Combiner | None,
    use_combiner: bool,
    control_queue: Queue,
    result_queue: Queue,
    simulate_worker_crash: bool,
    failure_marker: str,
) -> None:
    """Execute one map task in its own OS process."""
    try:
        control_queue.put(("started", "map", task_id, os.getpid()))

        # Kill the process exactly once for a selected map task.
        # This lets the coordinator exercise its "worker died -> retry task" path.
        if simulate_worker_crash and task_id == 0 and not Path(failure_marker).exists():
            Path(failure_marker).touch()
            os._exit(17)

        mapped: list[KeyValue] = []
        for line_number, line in records:
            mapped.extend(mapper(line_number, line))

        after_combine = mapped
        if use_combiner and combiner is not None:
            grouped: defaultdict[Any, list[Any]] = defaultdict(list)
            for key, value in mapped:
                grouped[key].append(value)
            after_combine = []
            for key, values in grouped.items():
                after_combine.extend(_normalize_output(combiner(key, values)))

        task_dir = Path(work_dir) / "map"
        task_dir.mkdir(parents=True, exist_ok=True)
        counts = 0
        bytes_written = 0
        for reducer_id in range(num_reducers):
            records_for_reducer = [
                (key, value)
                for key, value in after_combine
                if _partition(key, num_reducers) == reducer_id
            ]
            output = task_dir / f"map-{task_id:04d}-part-{reducer_id:04d}.jsonl"
            counts += _write_jsonl(output, records_for_reducer)
            if output.exists():
                bytes_written += output.stat().st_size

        result_queue.put(
            ("success", "map", task_id, len(mapped), len(after_combine), counts, bytes_written)
        )
    except Exception as exc:
        result_queue.put(("failure", "map", task_id, repr(exc)))


def _reduce_worker(
    task_id: int,
    map_task_count: int,
    work_dir: str,
    reducer: Reducer,
    result_queue: Queue,
) -> None:
    try:
        result_queue.put(("started", "reduce", task_id, os.getpid()))
        grouped: defaultdict[Any, list[Any]] = defaultdict(list)
        map_dir = Path(work_dir) / "map"
        for map_id in range(map_task_count):
            path = map_dir / f"map-{map_id:04d}-part-{task_id:04d}.jsonl"
            if not path.exists():
                raise FileNotFoundError(path)
            for key, value in _read_jsonl(path):
                grouped[key].append(value)

        output_dir = Path(work_dir) / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        output = output_dir / f"part-{task_id:04d}.jsonl"
        output_records: list[KeyValue] = []
        for key in sorted(grouped, key=lambda x: str(x)):
            output_records.extend(_normalize_output(reducer(key, grouped[key])))
        count = _write_jsonl(output, output_records)
        result_queue.put(("success", "reduce", task_id, len(grouped), count))
    except Exception as exc:
        result_queue.put(("failure", "reduce", task_id, repr(exc)))


class MiniMapReduce:
    """A learning-oriented MapReduce engine with real local processes.

    It models several ideas from the original MapReduce design:
      split → map tasks → local combine → partition → shuffle files → reduce tasks
      coordinator-style scheduling → task retries

    It intentionally stops short of networked workers and distributed storage.
    """

    def __init__(self, config: MapReduceConfig | None = None) -> None:
        self.config = config or MapReduceConfig()
        if self.config.num_map_tasks < 1 or self.config.num_reduce_tasks < 1:
            raise ValueError("num_map_tasks and num_reduce_tasks must be >= 1")
        if self.config.num_workers < 1:
            raise ValueError("num_workers must be >= 1")
        random.seed(self.config.seed)

    def run(
        self,
        input_path: str | Path,
        mapper: Mapper,
        reducer: Reducer,
        combiner: Combiner | None = None,
    ) -> tuple[list[KeyValue], JobStats]:
        started_at = time.perf_counter()
        work_dir = Path(self.config.work_dir)
        if work_dir.exists():
            shutil.rmtree(work_dir)
        (work_dir / "map").mkdir(parents=True)
        (work_dir / "output").mkdir(parents=True)

        splits = self._split_file(input_path, self.config.num_map_tasks)
        stats = JobStats(
            input_records=sum(len(s) for s in splits),
            map_tasks=len(splits),
            reduce_tasks=self.config.num_reduce_tasks,
        )

        print(f"[MASTER] input split into {len(splits)} map tasks")
        self._run_map_stage(splits, mapper, combiner, stats, work_dir)

        print("[MASTER] map stage complete; shuffle materialized as partition files")
        self._print_shuffle_summary(work_dir, stats)

        self._run_reduce_stage(reducer, stats, work_dir)

        results = []
        for rid in range(self.config.num_reduce_tasks):
            path = work_dir / "output" / f"part-{rid:04d}.jsonl"
            results.extend(_read_jsonl(path))

        stats.output_records = len(results)
        stats.elapsed_seconds = time.perf_counter() - started_at
        return results, stats

    def _run_map_stage(
        self,
        splits: list[list[tuple[int, str]]],
        mapper: Mapper,
        combiner: Combiner | None,
        stats: JobStats,
        work_dir: Path,
    ) -> None:
        pending = set(range(len(splits)))
        attempts = defaultdict(int)
        active: dict[int, tuple[int, float]] = {}
        processes: dict[int, Process] = {}
        result_q: Queue = Queue()
        control_q: Queue = Queue()
        available = list(range(self.config.num_workers))
        failure_marker = str(work_dir / ".failure_once")

        while pending or active:
            while pending and available:
                task_id = min(pending)
                pending.remove(task_id)
                worker_id = available.pop()
                attempts[task_id] += 1
                p = Process(
                    target=_map_worker,
                    args=(
                        task_id,
                        splits[task_id],
                        self.config.num_reduce_tasks,
                        str(work_dir),
                        mapper,
                        combiner,
                        self.config.use_combiner,
                        control_q,
                        result_q,
                        self.config.simulate_worker_crash,
                        failure_marker,
                    ),
                    name=f"map-worker-{worker_id}-task-{task_id}",
                )
                p.start()
                processes[worker_id] = p
                active[worker_id] = (task_id, time.monotonic())
                print(f"[MASTER] assigned MAP task {task_id} → worker-{worker_id} (attempt {attempts[task_id]})")

            # Harvest results / detect completed processes.
            made_progress = False
            try:
                while True:
                    msg = result_q.get_nowait()
                    kind, stage, task_id, *payload = msg
                    if kind == "started":
                        print(f"[{stage.upper()}-{task_id}] started in pid {payload[0]}")
                        made_progress = True
                        continue
                    if kind == "success":
                        for worker_id, active_info in list(active.items()):
                            if active_info[0] == task_id:
                                p = processes.pop(worker_id)
                                p.join()
                                active.pop(worker_id)
                                available.append(worker_id)
                                break
                        _, _, _, _, _, _, _ = msg
                        stats.mapper_records += payload[0]
                        stats.records_after_combiner += payload[1]
                        print(
                            f"[MAP-{task_id}] done: emitted={payload[0]}, "
                            f"post-combine={payload[1]}, shuffled={payload[2]}"
                        )
                        made_progress = True
                    elif kind == "failure":
                        for worker_id, active_info in list(active.items()):
                            if active_info[0] == task_id:
                                p = processes.pop(worker_id)
                                p.join()
                                active.pop(worker_id)
                                available.append(worker_id)
                                break
                        if attempts[task_id] <= self.config.max_retries:
                            pending.add(task_id)
                            stats.retries += 1
                            # Remove partial outputs from the failed attempt.
                            for part in (work_dir / "map").glob(f"map-{task_id:04d}-part-*.jsonl"):
                                part.unlink(missing_ok=True)
                            print(f"[MASTER] MAP task {task_id} failed ({payload[0]}), retrying")
                        else:
                            raise RuntimeError(f"map task {task_id} exceeded retry limit: {payload[0]}")
                        made_progress = True
            except queue.Empty:
                pass

            now = time.monotonic()
            for worker_id, (task_id, started) in list(active.items()):
                p = processes[worker_id]
                if not p.is_alive() and p.exitcode != 0:
                    processes.pop(worker_id)
                    active.pop(worker_id)
                    available.append(worker_id)
                    if attempts[task_id] <= self.config.max_retries:
                        pending.add(task_id)
                        stats.retries += 1
                        for part in (work_dir / "map").glob(f"map-{task_id:04d}-part-*.jsonl"):
                            part.unlink(missing_ok=True)
                        print(f"[MASTER] worker-{worker_id} died on MAP task {task_id}; reassigning")
                    else:
                        raise RuntimeError(f"worker died repeatedly on map task {task_id}")
                    made_progress = True
                elif now - started > self.config.task_timeout:
                    print(f"[MASTER] MAP task {task_id} timed out; terminating and retrying")
                    p.terminate()
                    p.join()
                    processes.pop(worker_id)
                    active.pop(worker_id)
                    available.append(worker_id)
                    if attempts[task_id] <= self.config.max_retries:
                        pending.add(task_id)
                        stats.retries += 1
                        for part in (work_dir / "map").glob(f"map-{task_id:04d}-part-*.jsonl"):
                            part.unlink(missing_ok=True)
                    else:
                        raise TimeoutError(f"map task {task_id} exceeded retry limit")
                    made_progress = True

            if not made_progress:
                time.sleep(0.02)

        # Clean stale attempt marker only after the successful job.
        Path(failure_marker).unlink(missing_ok=True)

    def _run_reduce_stage(self, reducer: Reducer, stats: JobStats, work_dir: Path) -> None:
        pending = set(range(self.config.num_reduce_tasks))
        attempts = defaultdict(int)
        active: dict[int, tuple[int, float]] = {}
        processes: dict[int, Process] = {}
        result_q: Queue = Queue()
        available = list(range(min(self.config.num_workers, self.config.num_reduce_tasks)))

        while pending or active:
            while pending and available:
                task_id = min(pending)
                pending.remove(task_id)
                worker_id = available.pop()
                attempts[task_id] += 1
                p = Process(
                    target=_reduce_worker,
                    args=(task_id, stats.map_tasks, str(work_dir), reducer, result_q),
                    name=f"reduce-worker-{worker_id}-task-{task_id}",
                )
                p.start()
                processes[worker_id] = p
                active[worker_id] = (task_id, time.monotonic())
                print(f"[MASTER] assigned REDUCE task {task_id} → worker-{worker_id}")

            made_progress = False
            try:
                while True:
                    msg = result_q.get_nowait()
                    kind, stage, task_id, *payload = msg
                    if kind == "started":
                        print(f"[{stage.upper()}-{task_id}] started in pid {payload[0]}")
                        made_progress = True
                        continue
                    for worker_id, active_info in list(active.items()):
                        if active_info[0] == task_id:
                            p = processes.pop(worker_id)
                            p.join()
                            active.pop(worker_id)
                            available.append(worker_id)
                            break
                    if kind == "success":
                        print(f"[REDUCE-{task_id}] done: unique_keys={payload[0]}, output={payload[1]}")
                    else:
                        if attempts[task_id] <= self.config.max_retries:
                            pending.add(task_id)
                            stats.retries += 1
                            print(f"[MASTER] REDUCE task {task_id} failed ({payload[0]}), retrying")
                        else:
                            raise RuntimeError(f"reduce task {task_id} exceeded retry limit: {payload[0]}")
                    made_progress = True
            except queue.Empty:
                pass

            for worker_id, (task_id, started) in list(active.items()):
                p = processes[worker_id]
                if not p.is_alive() and p.exitcode != 0:
                    processes.pop(worker_id)
                    active.pop(worker_id)
                    available.append(worker_id)
                    if attempts[task_id] <= self.config.max_retries:
                        pending.add(task_id)
                        stats.retries += 1
                        print(f"[MASTER] worker-{worker_id} died on REDUCE task {task_id}; reassigning")
                    else:
                        raise RuntimeError(f"worker died repeatedly on reduce task {task_id}")
                    made_progress = True

            if not made_progress:
                time.sleep(0.02)

    def _print_shuffle_summary(self, work_dir: Path, stats: JobStats) -> None:
        files = sorted((work_dir / "map").glob("*.jsonl"))
        stats.shuffle_bytes = sum(p.stat().st_size for p in files)
        stats.shuffle_records = 0
        for path in files:
            with path.open("r", encoding="utf-8") as f:
                stats.shuffle_records += sum(1 for _ in f)
        by_reducer = []
        for rid in range(self.config.num_reduce_tasks):
            count = sum(
                sum(1 for _ in p.open("r", encoding="utf-8"))
                for p in (work_dir / "map").glob(f"*-part-{rid:04d}.jsonl")
            )
            by_reducer.append(count)
        print(f"[SHUFFLE] {stats.shuffle_records} records, {stats.shuffle_bytes:,} bytes")
        print("[SHUFFLE] reducer partition sizes:", by_reducer)

    @staticmethod
    def _split_file(path: str | Path, num_splits: int) -> list[list[tuple[int, str]]]:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
        if not lines:
            return [[]]
        num_splits = max(1, min(num_splits, len(lines)))
        splits: list[list[tuple[int, str]]] = [[] for _ in range(num_splits)]
        # Contiguous chunks model input splits better than round-robin assignment.
        chunk = (len(lines) + num_splits - 1) // num_splits
        for task_id in range(num_splits):
            start = task_id * chunk
            end = min(len(lines), start + chunk)
            splits[task_id] = [(i, lines[i]) for i in range(start, end)]
        return splits
