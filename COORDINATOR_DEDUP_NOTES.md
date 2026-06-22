I investigated the next step for full coordinator-side row-level dedup.

Status update:
- The previous equality-delete approach was not sufficient to guarantee correct same-commit winner/loser behavior for duplicates spread across newly written files.
- Equality deletes depend on sequence-number ordering, but when duplicate rows coexist across the same commit's newly produced files, the coordinator cannot guarantee that delete files remove only the losing rows without also relying on file/row separation that does not generally exist.

Current direction:
- Rework coordinator-side dedup to a rewrite-based implementation.
- Read rows from all incoming data files in the commit.
- Extract PK tuples using `lakers.id-cols`.
- Keep only the newest row per PK.
- Rewrite surviving rows into replacement files.
- Commit rewritten files instead of originals.

Why rewrite is needed:
1. It guarantees exact row-level dedup correctness.
2. It avoids same-commit equality-delete sequence-number ambiguity.
3. It works even when winning and losing rows share the same original data file.

Additional cleanup needed:
- Extract shared writer/appender/id-column helper logic from `Utilities.createTableWriter` instead of reimplementing it in the coordinator path.
- Remove the in-progress equality-delete implementation once rewrite-based dedup replaces it.
