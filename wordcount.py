from __future__ import annotations

import argparse

from mapreduce import MiniMapReduce, MapReduceConfig


def mapper(_line_number: int, line: str):
    """Emit (word, 1) for every normalized word."""
    for raw in line.lower().split():
        word = "".join(ch for ch in raw if ch.isalnum())
        if word:
            yield word, 1


def combiner(word: str, counts):
    """Optional local aggregation: reduces records sent through shuffle."""
    return word, sum(counts)


def reducer(word: str, counts):
    """Final aggregation for one key."""
    return word, sum(counts)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Word Count on MiniMapReduce.")
    parser.add_argument("--maps", type=int, default=4)
    parser.add_argument("--reduces", type=int, default=2)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--no-combiner", action="store_true")
    parser.add_argument("--crash-demo", action="store_true", help="Crash map task 0 once to demonstrate retry.")
    parser.add_argument("--input", default="input.txt")
    args = parser.parse_args()

    config = MapReduceConfig(
        num_map_tasks=args.maps,
        num_reduce_tasks=args.reduces,
        num_workers=args.workers,
        use_combiner=not args.no_combiner,
        work_dir="mr_work",
        simulate_worker_crash=args.crash_demo,
    )

    engine = MiniMapReduce(config)
    results, stats = engine.run(args.input, mapper, reducer, combiner)

    print("\n=== FINAL OUTPUT ===")
    for word, count in results:
        print(f"{word}\t{count}")

    print("\n" + stats.pretty())
    print("\nIntermediate files are available under ./mr_work/map/")


if __name__ == "__main__":
    main()
