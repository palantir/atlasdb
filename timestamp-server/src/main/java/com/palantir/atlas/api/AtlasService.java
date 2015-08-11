package com.palantir.atlas.api;

import java.util.Set;

import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.annotation.Idempotent;

public interface AtlasService {

    @Idempotent
    Set<String> getAllTableNames();

    @Idempotent
    TableMetadata getTableMetadata(String tableName);

    @Idempotent
    TransactionToken startTransaction();

    @Idempotent
    TableRowResult getRows(TransactionToken token,
                           TableRowSelection rows);

    @Idempotent
    TableCellVal getCells(TransactionToken token,
                          TableCell cells);

    @Idempotent
    RangeToken getRange(TransactionToken token,
                        TableRange rangeRequest);

    @Idempotent
    void put(TransactionToken token,
             TableCellVal data);

    @Idempotent
    void delete(TransactionToken token,
                TableCell cells);

    @Idempotent
    void commit(TransactionToken token);

    @Idempotent
    void abort(TransactionToken token);
}
