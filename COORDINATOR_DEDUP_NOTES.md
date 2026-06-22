I investigated the next step for full coordinator-side row-level dedup.

Current blocker:
- The repository does not contain an existing coordinator-side Iceberg data-file reader or rewrite helper to adapt.
- Implementing true dedup now requires introducing new Iceberg read/rewrite infrastructure, not just wiring existing repo code.

What is currently in branch `lakers`:
- heuristic coordinator-side file filtering in `Coordinator.java`
- not true row-level PK dedup

Needed for full implementation:
1. Read records from `DataFile`s at coordinator time.
2. Extract PK tuples using `lakers.id-cols`.
3. Rewrite surviving rows into replacement files.
4. Commit rewritten files instead of originals.
5. Add readable-file tests.

This note is being added only to preserve investigation context.
