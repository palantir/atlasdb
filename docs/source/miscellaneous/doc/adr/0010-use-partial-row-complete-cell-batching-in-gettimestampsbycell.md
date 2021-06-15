10. Use partial row complete cell batching in getTimestampsByCell
*****************************************************************

Date: 10/03/2017

## Status

Accepted

## Context

As of version 0.35.0 , our implementation of `DbKvs.getTimestampsByCell` was creating a MultiMap with all pairs of
(Cell, timestamp) from the range determined by the row batch size. In case of wide rows, or simply a row batch size
that is too large, this could cause us to run out of memory; see [issue #982](https://github.com/palantir/atlasdb/issues/982).

## Decision

We decided to use a block (a triple of row name, column name, and timestamp) batch size, defaulting to 1000000 to avoid
loading too many blocks into memory. The algorithm will fetch blocks by row, column, and increasing timestamp until the
batch size is met. In that case:

1. If at least one row was fully processed, we return RowResults with all timestamps each of those rows, and continue
    processing from the beginning of current row when more RowResults are necessary.
2. If no rows were fully processed, we continue processing blocks until the end of the current cell (row, column),
    fetching all timestamps for that cell. We return a (partial) RowResult that contains all timestamps for that row
    until and including the final cell processed. To create the next RowResult we then continue processing the row after
    the last cell processed, effectively splitting the wide row into several RowResults.

Example:
Assuming a batch size of 10 and the following table

  |   |         1          |          2         |     3     |
  |   | ------------------ | ------------------ | --------- |
  | 1 |     (1, 2, 3)      |      (4, 5, 6)     | (7, 8, 9) |
  | 2 |    (1, 2, 3, 4)    |    (4, 5, 6, 7)    | (7, 8, 9) |
  | 3 | (1, 2, 3, 4, 5, 6) | (4, 5, 6, 7, 8, 9) | (7, 8, 9) |
  | 4 |     (1, 2, 3)      |                    |           |

The RowResults will be as follows:
- (1 -> (1 -> (1, 2, 3); 2 -> (4, 5, 6); 3 -> (7, 8, 9)))
- (2 -> (1 -> (1, 2, 3, 4); 2 -> (4, 5, 6, 7); 3 -> (7, 8, 9)))
- (3 -> (1 -> (1, 2, 3, 4, 5, 6); 2 -> (4, 5, 6, 7, 8, 9)))
- (3 -> (3 -> (7, 8, 9)))
- (4 -> (1 -> (1, 2, 3)))

Other options considered:

1. Partial row batching: fetch up to 1000000 blocks. If the block batch size is hit, stop immediately (cells can be split across multiple batches).
2. Full row batching: fetch up to 1000000 blocks. If the block batch size is hit in the middle of a row, continue fetching until the row is exhausted.
3. No batching, but throw an error when the block batch hint is reached

- Option 1 was considered, but was replaced by the modified version above. This is because sweep must ensure that all blocks
  except for the most recent (before the immutable timestamp) are deleted. This can be achieved by repeating the last
  block from one batch in the next batch, or by keeping this last result in sweep's memory, so that it knows to remove it,
  if further blocks for the same cell are encountered. Neither option is compelling: the first is hard to reason about,
  and the second reduces the scope for parallelisation, and risks introducing a correctness bug.
- Option 2 was not chosen, because it does not guard well against wide rows (many cells) that have many overwrites.
- Option 3 was considered, but was ultimately discarded because it relies on properties of the code that calls getCellTimestamps.
  In particular, there needs to be retry logic that detects that an error was thrown, and reduces the batch size accordingly.

## Consequences

For rows wider than the value of the batch size, `DbKvs.getTimestampsByCell` may split them into several RowResults.
