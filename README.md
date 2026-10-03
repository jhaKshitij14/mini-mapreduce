# MiniMapReduce

A learning-oriented MapReduce implementation inspired by the original Google MapReduce design.

This version is deliberately more substantial than a toy `map()` + `reduce()` script:

```text
                         MASTER / COORDINATOR
                                  |
                 +----------------+----------------+
                 |                                 |
             MAP TASKS                         REDUCE TASKS
                 |                                 ^
     +-----------+-----------+                     |
     |           |           |                     |
  process     process     process  ...             |
     |           |           |                     |
     +-----------+-----------+                     |
                 |                                 |
          LOCAL COMBINER                           |
                 |                                 |
          PARTITION BY KEY                         |
                 |                                 |
      partition files on disk  --------------------+
                 |
              SHUFFLE
                 |
          grouped by key
                 |
             REDUCERS
                 |
             output files
```

## What this implementation demonstrates

- Input splitting into map tasks.
- Map tasks running in **separate OS processes**.
- Optional local combiner.
- Deterministic hash partitioning.
- Materialized intermediate partition files, making the shuffle visible.
- Separate reduce processes.
- A simple coordinator that schedules tasks into worker slots and tracks attempts.
- Retry after a task failure or actual worker-process death.
- Task timeout handling.
- Shuffle size / record statistics.
- No third-party dependencies.

## Run

From this directory:

```bash
python wordcount.py
```

Run the normal job with:

```bash
python wordcount.py
```

To deliberately kill the process executing map task 0 once:

```bash
python wordcount.py --crash-demo
```

The coordinator detects the non-zero process exit and reassigns the task.

## Inspect the shuffle

After running, look at:

```text
mr_work/map/
```

You will see files like:

```text
map-0000-part-0000.jsonl
map-0000-part-0001.jsonl
map-0001-part-0000.jsonl
...
```

Each `part-N` file is the data that a particular reducer will consume from one mapper. That is the physical representation of the shuffle in this mini-system.