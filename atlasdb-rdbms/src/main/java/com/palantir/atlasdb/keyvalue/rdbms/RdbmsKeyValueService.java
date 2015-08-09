package com.palantir.atlasdb.keyvalue.rdbms;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcConnectionPool;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.StringMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.util.Pair;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public final class RdbmsKeyValueService extends AbstractKeyValueService {

    private static final Logger log = LoggerFactory.getLogger(RdbmsKeyValueService.class);

    private static final class Columns {
        public static final String TIMESTAMP = "timestamp";
        public static final String CONTENT = "content";
        public static final String ROW = "row";
        public static final String COLUMN = "column";
        public static final String all() {
            return ROW + ", " + COLUMN + ", " + TIMESTAMP + ", " + CONTENT;
        }
    }

    private static final class MetaTable {
        public static final String META_TABLE_NAME = "_table_meta";

        private static final class Columns {
            public static final String TABLE_NAME = "table_name";
            public static final String METADATA = "metadata";
        }
    }

    private final DBI dbi;

    private DBI getDbi() {
        return dbi;
    }

    private DataSource getTestDataSource() {
        return JdbcConnectionPool.create("jdbc:h2:mem:test", "username", "password");
    }

    public RdbmsKeyValueService(ExecutorService executor) {
        super(executor);
        dbi = new DBI(getTestDataSource());
    }

    @Override
    public void initializeFromFreshInstance() {
        getDbi().inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
                try {
                    // Silently ignore if already initialized
                    handle.select("SELECT 1 FROM " + MetaTable.META_TABLE_NAME);
                    log.warn("Initializing an already initialized rdbms kvs. Ignoring.");
                } catch (RuntimeException e) {
                    log.warn("Initializing from fresh instance " + this);
                    handle.execute("CREATE TABLE " + MetaTable.META_TABLE_NAME + " ("
                            + MetaTable.Columns.TABLE_NAME + " VARCHAR(128), "
                            + MetaTable.Columns.METADATA + " BYTEA NOT NULL)");
                }
                return null;
            }
        });
    }

    private String makeSlots(String prefix, int number) {
        String result = "";
        for (int i=0; i<number; ++i) {
            result += ":" + prefix + i;
            if (i + 1 < number) {
                result += ", ";
            }
        }
        return result;
    }

    @Override
    @Idempotent
    public Map<Cell, Value> getRows(final String tableName,
                                    final Iterable<byte[]> rows,
                                    final ColumnSelection columnSelection,
                                    final long timestamp) {
        return getDbi().inTransaction(new TransactionCallback<Map<Cell, Value>>() {
            @Override
            public Map<Cell, Value> inTransaction(Handle handle, TransactionStatus status) {

                final List<Pair<Cell,Value>> list;
                final Map<Cell, Value> result = Maps.newHashMap();

                    handle.execute("DECLARE TABLE RowsToRetrieve (row BYTEA NOT NULL)");

                PreparedBatch batch = handle.prepareBatch("INSERT INTO RowsToRetrieve (row) VALUES (:row)");
                for (byte[] row : rows) {
                    batch.add(row);
                }
                batch.execute();

                if (columnSelection.allColumnsSelected()) {

                    list = handle.createQuery(
                            "SELECT t.row as row, t.column as column, t.timestamp as timestamp, t.content as content " +
                            "FROM " + tableName + " t " +
                            "JOIN RowsToRetrieve c " +
                            "ON t.row = c.row AND t.timestamp < " + timestamp + " " +
                            "LEFT JOIN " + tableName + " t2 " +
                    		"ON t.row = t2.row AND t.column = t2.column " +
                    		"    AND t.timestamp < t2.timestamp AND t2.timestamp < " + timestamp + " " +
                            "WHERE t2.timestamp IS NULL")
                            .map(CellValueMapper.instance()).list();
                } else {
                    handle.execute("DECLARE TABLE ColumnsToRetrieve (column BYTEA NOT NULL)");
                    PreparedBatch columnBatch = handle.prepareBatch("INSERT INTO ColumnsToRetrieve (column) VALUES (:column)");
                    for (byte[] column : columnSelection.getSelectedColumns()) {
                        columnBatch.add(column);
                    }
                    columnBatch.execute();

                    list = handle.createQuery(
                            "SELECT t.row as row, t.column as column, t.timestamp as timestamp, t.content as content " +
                            "FROM " + tableName + " t " +
                    		"JOIN RowsToRetrieve c " +
                    		"ON t.row = c.row AND t.timestamp < " + timestamp + " " +
                    		"LEFT JOIN " + tableName + " t2 " +
                    		"ON t.row = t2.row AND t.column = t2.column " +
                    		"    AND t.timestamp < t2.timestamp AND t2.timestamp < " + timestamp + " " +
                    		"WHERE t2.timestamp IS NULL " +
                    		"    AND t.column IN (SELECT column FROM ColumnsToRetrieve)"
                            ).map(CellValueMapper.instance()).list();
                }

                for (Pair<Cell, Value> cv : list) {
                    Preconditions.checkState(!result.containsKey(cv.getLhSide()));
                    result.put(cv.getLhSide(), cv.getRhSide());
                }

                handle.execute("DROP TABLE RowsToRetrieve");
                if (!columnSelection.allColumnsSelected()) {
                    handle.execute("DROP TABLE ColumnsToRetrieve");
                }

                return result;
            }
        });
    }

    private static class ValueMapper implements ResultSetMapper<Value> {

        private static ValueMapper instance;

        @Override
        public Value map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            long timestamp = r.getLong(Columns.TIMESTAMP);
            byte[] content = r.getBytes(Columns.CONTENT);
            return Value.create(content, timestamp);
        }

        private static ValueMapper instance() {
            if (instance == null) {
                ValueMapper ret = new ValueMapper();
                instance = ret;
            }
            return instance;
        }
    }

    private static class RowResultMapper implements ResultSetMapper<RowResult<Value>> {
        private static RowResultMapper instance;

        @Override
        public RowResult<Value> map(int index, ResultSet r, StatementContext ctx)
                throws SQLException {
            byte[] row = r.getBytes(Columns.ROW);
            byte[] col = r.getBytes(Columns.COLUMN);
            long timestamp = r.getLong(Columns.TIMESTAMP);
            byte[] content = r.getBytes(Columns.CONTENT);
            return RowResult.of(Cell.create(row, col), Value.create(content, timestamp));
        }

        private static RowResultMapper instance() {
            if (instance == null) {
                RowResultMapper ret = new RowResultMapper();
                instance = ret;
            }
            return instance;
        }
    }

    private static class CellValueMapper implements ResultSetMapper<Pair<Cell, Value>> {
        private static CellValueMapper instance;

        @Override
        public Pair<Cell, Value> map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            byte[] row = r.getBytes(Columns.ROW);
            byte[] col = r.getBytes(Columns.COLUMN);
            long timestamp = r.getLong(Columns.TIMESTAMP);
            byte[] content = r.getBytes(Columns.CONTENT);
            Cell cell = Cell.create(row, col);
            Value value = Value.create(content, timestamp);
            return Pair.create(cell, value);
        }

        private static CellValueMapper instance() {
            if (instance == null) {
                CellValueMapper ret = new CellValueMapper();
                instance = ret;
            }
            return instance;
        }
    }

    @Override
    @Idempotent
    public Map<Cell, Value> get(final String tableName, final Map<Cell, Long> timestampByCell) {
        return getDbi().withHandle(new HandleCallback<Map<Cell, Value>>() {
            @Override
            public Map<Cell, Value> withHandle(Handle handle) throws Exception {

                handle.execute("DECLARE TABLE CellsToRetrieve (row BYTEA NOT NULL, column BYTEA NOT NULL, timestamp INT NOT NULL)");

                PreparedBatch batch = handle.prepareBatch("INSERT INTO CellsToRetrieve (row, column, timestamp) VALUES (:row, :column, :timestamp)");
                for (Entry<Cell, Long> entry : timestampByCell.entrySet()) {
                    batch.add(entry.getKey().getRowName(), entry.getKey().getColumnName(), entry.getValue());
                }
                batch.execute();

                Map<Cell, Value> result = Maps.newHashMap();

                List<Pair<Cell, Value>> values = handle.createQuery(
                        "SELECT t.row AS row, t.column AS column, t.timestamp AS timestamp, t.content AS content " +
        		        "FROM " + tableName + " t " +
		        		"JOIN CellsToRetrieve c " +
		        		"ON t.row = c.row AND t.column = c.column AND t.timestamp < c.timestamp " +
		        		"LEFT JOIN " + tableName + " t2 " +
        				"ON t.row = t2.row AND t.column = t2.column AND t.timestamp < t2.timestamp " +
		        		"    AND t2.timestamp < c.timestamp " +
        				"WHERE t2.timestamp IS NULL")
        				.map(CellValueMapper.instance())
        				.list();

                handle.execute("DROP TABLE CellsToRetrieve");

                for (Pair<Cell, Value> cv : values) {
                    Preconditions.checkState(!result.containsKey(cv.getLhSide()));
                    result.put(cv.getLhSide(), cv.getRhSide());
                }

                return result;
            }
        });
    }

    @Override
    public void put(final String tableName, final Map<Cell, byte[]> values, final long timestamp)
            throws KeyAlreadyExistsException {
        // TODO: Throw the KeyAlreadyExistsException when appropriate
        getDbi().withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {

                handle.execute("DECLARE TABLE ValuesToPut (row BYTEA NOT NULL, column BYTEA NOT NULL, content BYTEA NOT NULL)");

                PreparedBatch batch = handle.prepareBatch("INSERT INTO ValuesToPut (row, column, content) VALUES (:row, :column, :content)");
                for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
                    batch.add(e.getKey().getRowName(), e.getKey().getColumnName(), e.getValue());
                }
                batch.execute();

                handle.execute(
                        "INSERT INTO " + tableName + " (row, column, timestamp, content) " +
                		"SELECT row, column, " + timestamp + ", content FROM ValuesToPut");

                handle.execute("DROP TABLE ValuesToPut");

                return null;
            }
        });
    }

    @Override
    @NonIdempotent
    public void putWithTimestamps(final String tableName, final Multimap<Cell, Value> cellValues)
            throws KeyAlreadyExistsException {
        // TODO: Throw the KeyAlreadyExistsException when appropriate
        getDbi().withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {

                handle.execute(
                        "DECLARE TABLE ValuesToPut (" +
                        "    row BYTEA NOT NULL, " +
                        "    column BYTEA NOT NULL, " +
                        "    timestamp INT NOT NULL, " +
                        "    content BYTEA NOT NULL)");

                PreparedBatch batch = handle.prepareBatch(
                        "INSERT INTO ValuesToPut (row, column, timestamp, content) " +
                        "VALUES (:row, :column, :timestamp, :content)");
                for (Entry<Cell, Value> e : cellValues.entries()) {
                    batch.add(
                            e.getKey().getRowName(),
                            e.getKey().getColumnName(),
                            e.getValue().getTimestamp(),
                            e.getValue().getContents());
                }
                batch.execute();

                handle.execute(
                        "INSERT INTO " + tableName + " (row, column, timestamp, content) " +
                		"SELECT row, column, timestamp, content FROM ValuesToPut");

                handle.execute("DROP TABLE ValuesToPut");

                return null;
            }
        });
    }

    @Override
    public void putUnlessExists(final String tableName, final Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        put(tableName, values, 0);
    }

    @Override
    @Idempotent
    public void delete(final String tableName, final Multimap<Cell, Long> keys) {
        getDbi().withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                handle.execute("DECLARE TABLE CellsToDelete (row BYTEA NOT NULL, column BYTEA NOT NULL, timestamp INT NOT NULL)");
                PreparedBatch batch = handle.prepareBatch("INSERT INTO CellsToDelete (row, column, timestamp) VALUES (:row, :column, :timestamp)");
                for (Entry<Cell, Long> entry : keys.entries()) {
                    batch.add(entry.getKey().getRowName(), entry.getKey().getColumnName(), entry.getValue());
                }
                batch.execute();

                handle.execute(
                        "DELETE FROM " + tableName + " t " +
                		"WHERE EXISTS (" +
                		"    SELECT 1 FROM CellsToDelete c " +
                		"    WHERE t.row = c.row AND t.column = c.column " +
                		"        AND t.timestamp = c.timestamp " +
                		"    LIMIT 1)");

                handle.execute("DROP TABLE CellsToDelete");
                return null;
            }
        });
    }

    private int getMaxRows(RangeRequest rangeRequest) {
        return rangeRequest.getBatchHint() == null ? 100 : rangeRequest.getBatchHint();
    }

    private TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getPage(final String tableName,
                                                                          final RangeRequest rangeRequest,
                                                                          final long timestamp) {
        Preconditions.checkArgument(!rangeRequest.isReverse());
        final int maxRows = getMaxRows(rangeRequest);
        return getDbi().withHandle(new HandleCallback<TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>() {
            @Override
            public TokenBackedBasicResultsPage<RowResult<Value>, byte[]> withHandle(Handle handle) throws Exception {
                List<byte[]> rows = handle.createQuery(
                        "SELECT DISTINCT row FROM " + tableName + " WHERE row >= :startRow AND timestamp < :timestamp LIMIT :limit")
                        .bind("startRow", rangeRequest.getStartInclusive())
                        .bind("timestamp", timestamp)
                        .bind("limit", maxRows)
                        .map(ByteArrayMapper.FIRST)
                        .list();
                if (rows.isEmpty()) {
                            return new SimpleTokenBackedResultsPage<RowResult<Value>, byte[]>(
                                    rangeRequest.getStartInclusive(),
                                    Collections.<RowResult<Value>> emptyList(),
                                    false);
                }
                Map<Cell, Value> cells = getRows(tableName, rows, ColumnSelection.all(), timestamp);
                NavigableMap<byte[], SortedMap<byte[], Value>> byRow = Cells.breakCellsUpByRow(cells);
                List<RowResult<Value>> finalResult = Lists.newArrayList();
                for (Entry<byte[], SortedMap<byte[], Value>> e : byRow.entrySet()) {
                    finalResult.add(RowResult.create(e.getKey(), e.getValue()));
                }
                byte[] token = RangeRequests.getNextStartRow(rangeRequest.isReverse(), rows.get(rows.size() - 1));
                return new SimpleTokenBackedResultsPage<RowResult<Value>, byte[]>(token, finalResult, rows.size() == maxRows);
            }
        });
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Value>> getRange(final String tableName,
                                                       final RangeRequest rangeRequest,
                                                       final long timestamp) {
        return ClosableIterators.wrap(new AbstractPagingIterable<RowResult<Value>, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>() {
            @Override
            protected TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getFirstPage()
                    throws Exception {
                return getPage(tableName, rangeRequest, timestamp);
            }

            @Override
            protected TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getNextPage(TokenBackedBasicResultsPage<RowResult<Value>, byte[]> previous)
                    throws Exception {
                return getPage(
                        tableName,
                        rangeRequest.getBuilder().startRowInclusive(previous.getTokenForNextPage()).build(),
                        timestamp);
            }
        }.iterator());
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp)
            throws InsufficientConsistencyException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Idempotent
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName,
                                                                                                           Iterable<RangeRequest> rangeRequests,
                                                                                                           long timestamp) {
        return KeyValueServices.getFirstBatchForRangesUsingGetRange(
                this,
                tableName,
                rangeRequests,
                timestamp);
    }

    @Override
    @Idempotent
    public void dropTable(final String tableName) throws InsufficientConsistencyException {
        getDbi().inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
                handle.execute("DROP TABLE " + tableName);
                handle.execute("DELETE FROM " + MetaTable.META_TABLE_NAME + " WHERE "
                        + MetaTable.Columns.TABLE_NAME + " = ?", tableName);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void createTable(final String tableName, int maxValueSizeInBytes)
            throws InsufficientConsistencyException {
        getDbi().inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
                handle.execute("CREATE TABLE " + tableName + " (" + Columns.ROW
                        + " BYTEA NOT NULL, " + Columns.COLUMN + " BYTEA NOT NULL, "
                        + Columns.TIMESTAMP + " INT NOT NULL, " + Columns.CONTENT
                        + " BYTEA, PRIMARY KEY (" + Columns.ROW + ", " + Columns.COLUMN + ", "
                        + Columns.TIMESTAMP + "))");
                handle.execute("INSERT INTO " + MetaTable.META_TABLE_NAME + " ("
                        + MetaTable.Columns.TABLE_NAME + ", " + MetaTable.Columns.METADATA
                        + ") VALUES (?, ?)", tableName, new byte[0]);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public Set<String> getAllTableNames() {
        return Sets.newHashSet(getDbi().withHandle(new HandleCallback<List<String>>() {
            @Override
            public List<String> withHandle(Handle handle) throws Exception {
                return handle.createQuery(
                        "SELECT " + MetaTable.Columns.TABLE_NAME + " FROM "
                                + MetaTable.META_TABLE_NAME).map(StringMapper.FIRST).list();
            }
        }));
    }

    @Override
    @Idempotent
    public byte[] getMetadataForTable(final String tableName) {
        return getDbi().inTransaction(new TransactionCallback<byte[]>() {
            @Override
            public byte[] inTransaction(Handle conn, TransactionStatus status) throws Exception {
                return conn.createQuery(
                        "SELECT " + MetaTable.Columns.METADATA + " FROM "
                                + MetaTable.META_TABLE_NAME + " WHERE "
                                + MetaTable.Columns.TABLE_NAME + " = :tableName").bind(
                        "tableName",
                        tableName).map(ByteArrayMapper.FIRST).first();
            }
        });
    }

    @Override
    @Idempotent
    public void putMetadataForTable(final String tableName, final byte[] metadata) {
        getDbi().inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle conn, TransactionStatus status) throws Exception {
                conn.execute("UPDATE " + MetaTable.META_TABLE_NAME + " SET "
                        + MetaTable.Columns.METADATA + " = ? WHERE " + MetaTable.Columns.TABLE_NAME
                        + " = ?", metadata, tableName);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void addGarbageCollectionSentinelValues(final String tableName, final Set<Cell> cells) {
        // TODO: Can it fail if the cell did not exist originally?
        getDbi().withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                for (Cell c : cells) {
                    handle.execute(
                            "UPDATE " + tableName + " SET (" + Columns.CONTENT + ", "
                                    + Columns.TIMESTAMP + ") VALUES (?, ?) WHERE " + Columns.ROW
                                    + "  = ? AND " + Columns.COLUMN + " = ?",
                            new byte[0],
                            Value.INVALID_VALUE_TIMESTAMP,
                            c.getRowName(),
                            c.getColumnName());
                }
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public Multimap<Cell, Long> getAllTimestamps(final String tableName,
                                                 final Set<Cell> cells,
                                                 final long timestamp)
            throws InsufficientConsistencyException {

        final String sqlQuery =
                "SELECT t.row as row, t.column as column, t.timestamp as timestamp " +
                "FROM " + tableName + " t " +
                "JOIN CellsToRetrieve c ON t.row = c.row AND t.column = c.column " +
                "WHERE t.timestamp < " + timestamp;

        return getDbi().withHandle(new HandleCallback<Multimap<Cell, Long>>() {
            @Override
            public Multimap<Cell, Long> withHandle(Handle handle) throws Exception {

                handle.execute(
                        "DECLARE TABLE CellsToRetrieve (" +
                        "row BYTEA NOT NULL, " +
                        "column BYTEA NOT NULL)");

                PreparedBatch batch = handle.prepareBatch(
                        "INSERT INTO CellsToRetrieve (" +
                        "row, column) VALUES (" +
                        ":row, :column)");
                for (Cell c : cells) {
                    batch.add(c.getRowName(), c.getColumnName());
                }
                batch.execute();

                List<Pair<Cell, Long>> sqlResult = handle.createQuery(sqlQuery)
                        .map(new ResultSetMapper<Pair<Cell, Long>>() {
                            @Override
                            public Pair<Cell, Long> map(int index, ResultSet r, StatementContext ctx)
                                    throws SQLException {
                                byte[] row = r.getBytes("row");
                                byte[] column = r.getBytes("column");
                                long timestamp = r.getLong("timestamp");
                                Cell cell = Cell.create(row, column);
                                return Pair.create(cell, timestamp);
                            }
                        }).list();


                Multimap<Cell, Long> result = HashMultimap.create();
                for (Pair<Cell, Long> p : sqlResult) {
                    result.put(p.getLhSide(), p.getRhSide());
                }

                handle.execute("DROP TABLE CellsToRetrieve");

                return result;
            }
        });
    }

    @Override
    public void compactInternally(String tableName) {
        // TODO Auto-generated method stub
    }

}
