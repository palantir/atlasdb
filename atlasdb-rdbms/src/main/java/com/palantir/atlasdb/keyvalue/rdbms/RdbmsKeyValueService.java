package com.palantir.atlasdb.keyvalue.rdbms;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcConnectionPool;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.StringMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public final class RdbmsKeyValueService extends AbstractKeyValueService {

    private static final Logger log = LoggerFactory.getLogger(RdbmsKeyValueService.class);

    private static final class Columns {
        public static final String TIMESTAMP = "timestamp";
        public static final String CONTENT = "content";
        public static final String ROW = "row";
        public static final String COLUMN = "column";
    }

    private static final class MetaTable {
        public static final String META_TABLE_NAME = "_table_meta";

        private static final class Columns {
            public static final String TABLE_NAME = "table_name";
            public static final String METADATA = "metadata";
        }
    }

    final DBI dbi;

    private DBI getDbi() {
        return dbi;
    }

    public RdbmsKeyValueService(ExecutorService executor) {
        super(executor);
        DataSource ds = JdbcConnectionPool.create("jdbc:h2:mem:test", "username", "password");
        dbi = new DBI(ds);
    }

    @Override
    public void initializeFromFreshInstance() {
        getDbi().inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
                try {
                    handle.select("SELECT 1 FROM " + MetaTable.META_TABLE_NAME);
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

    private static class SingleResult {
        final Cell cell;
        final Value value;

        static SingleResult of(Cell cell, Value value) {
            return new SingleResult(cell, value);
        }

        private SingleResult(Cell cell, Value value) {
            this.cell = cell;
            this.value = value;
        }

        Cell getCell() {
            return cell;
        }

        Value getValue() {
            return value;
        }
    }

    @Override
    @Idempotent
    public Map<Cell, Value> getRows(final String tableName,
                                    final Iterable<byte[]> rows,
                                    final ColumnSelection columnSelection,
                                    long timestamp) {
        return getDbi().withHandle(new HandleCallback<Map<Cell, Value>>() {
            @Override
            public Map<Cell, Value> withHandle(Handle handle) throws Exception {

                Map<Cell, Value> result = Maps.newHashMap();

                for (byte[] row : rows) {
                    List<SingleResult> cells = handle.createQuery(
                            "SELECT " + Columns.ROW + ", " + Columns.COLUMN + ", "
                                    + Columns.CONTENT + ", " + Columns.TIMESTAMP + " FROM "
                                    + tableName + " WHERE " + Columns.ROW + " = :row").bind(
                            "row",
                            row).map(RowResultMapper.instance()).list();
                    for (SingleResult r : cells) {
                        if (columnSelection.contains(r.getCell().getColumnName())) {
                            result.put(r.getCell(), r.getValue());
                        }
                    }
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

        private ValueMapper() {
            // Do NOT use this from outside instance() method...
            // TODO: Move to another file
        }

        private static ValueMapper instance() {
            if (instance == null) {
                ValueMapper ret = new ValueMapper();
                instance = ret;
            }
            return instance;
        }
    }

    private static class RowResultMapper implements ResultSetMapper<SingleResult> {
        private static RowResultMapper instance;

        @Override
        public SingleResult map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            byte[] row = r.getBytes(Columns.ROW);
            byte[] col = r.getBytes(Columns.COLUMN);
            long timestamp = r.getLong(Columns.TIMESTAMP);
            byte[] content = r.getBytes(Columns.CONTENT);
            return SingleResult.of(Cell.create(row, col), Value.create(content, timestamp));
        }

        private static RowResultMapper instance() {
            if (instance == null) {
                RowResultMapper ret = new RowResultMapper();
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
                Map<Cell, Value> result = Maps.newHashMap();
                for (Map.Entry<Cell, Long> e : timestampByCell.entrySet()) {
                    List<Value> values = handle.createQuery(
                            "SELECT content, timestamp FROM "
                                    + tableName
                                    + " WHERE row=:row AND column=:column AND timestamp < :timestamp").bind(
                            "row",
                            e.getKey().getRowName()).bind("column", e.getKey().getColumnName()).bind(
                            "timestamp",
                            e.getValue()).map(ValueMapper.instance()).list();
                    Value latestValue = values.get(0);
                    for (Value value : values) {
                        if (value.getTimestamp() > latestValue.getTimestamp()) {
                            latestValue = value;
                        }
                    }
                    result.put(e.getKey(), latestValue);
                }
                return result;
            }
        });
    }

    @Override
    public void put(final String tableName, final Map<Cell, byte[]> values, final long timestamp)
            throws KeyAlreadyExistsException {
        getDbi().withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
                    int result = handle.createStatement(
                            "INSERT INTO " + tableName + " (" + Columns.ROW + ", " + Columns.COLUMN
                                    + ", " + Columns.CONTENT + ", " + Columns.TIMESTAMP
                                    + ") VALUES (:row, :column, :content, :timestamp)").bind(
                            "row",
                            e.getKey().getRowName()).bind("column", e.getKey().getColumnName()).bind(
                            "content",
                            e.getValue()).bind("timestamp", timestamp).execute();
                    assert result == 1;
                }
                return null;
            }
        });
    }

    @Override
    @NonIdempotent
    public void putWithTimestamps(final String tableName, final Multimap<Cell, Value> cellValues)
            throws KeyAlreadyExistsException {
        getDbi().withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                for (Entry<Cell, Value> e : cellValues.entries()) {
                    int result = handle.createStatement(
                            "INSERT INTO " + tableName + " (" + Columns.ROW + ", " + Columns.COLUMN
                                    + ", " + Columns.CONTENT + ", " + Columns.TIMESTAMP
                                    + ") VALUES (:row, :column, :content, :timestamp)").bind(
                            "row",
                            e.getKey().getRowName()).bind("column", e.getKey().getColumnName()).bind(
                            "content",
                            e.getValue().getContents()).bind(
                            "timestamp",
                            e.getValue().getTimestamp()).execute();
                    assert result == 1;
                }
                return null;
            }
        });
    }

    @Override
    public void putUnlessExists(String tableName, Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        // TODO Auto-generated method stub

    }

    @Override
    @Idempotent
    public void delete(final String tableName, Multimap<Cell, Long> keys) {
        getDbi().withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                handle.execute("DROP TABLE " + tableName);
                handle.execute("DELETE FROM " + MetaTable.META_TABLE_NAME + " WHERE "
                        + MetaTable.Columns.TABLE_NAME + "=:tableName", tableName);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Value>> getRange(String tableName,
                                                       RangeRequest rangeRequest,
                                                       long timestamp) {
        // TODO Auto-generated method stub
        return null;
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
    public byte[] getMetadataForTable(String tableName) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Idempotent
    public void putMetadataForTable(String tableName, byte[] metadata) {
        // TODO Auto-generated method stub

    }

    @Override
    @Idempotent
    public void addGarbageCollectionSentinelValues(String tableName, Set<Cell> cells) {
        // TODO Auto-generated method stub

    }

    @Override
    @Idempotent
    public Multimap<Cell, Long> getAllTimestamps(String tableName, Set<Cell> cells, long timestamp)
            throws InsufficientConsistencyException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void compactInternally(String tableName) {
        // TODO Auto-generated method stub
    }

}
