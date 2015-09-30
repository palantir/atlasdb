/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.rocksdb.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import com.google.common.base.Charsets;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.collect.Maps2;
import com.palantir.util.MutuallyExclusiveSetLock;
import com.palantir.util.MutuallyExclusiveSetLock.LockState;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class RocksDbKeyValueService implements KeyValueService {
    private static final String METADATA_TABLE_NAME = "_metadata";
    private static final long PUT_UNLESS_EXISTS_TS = 0L;
    private static final String LOCK_FILE_PREFIX = ".pt_kv_lock";
    private final RocksDB db;
    private final Options options;
    private final ConcurrentMap<String, ColumnFamilyHandle> tables;
    private final FileLock lock;
    private final RandomAccessFile lockFile;
    private final MutuallyExclusiveSetLock<Cell> lockSet = MutuallyExclusiveSetLock.<Cell>create(false);
    private volatile boolean closed = false;

    public static RocksDbKeyValueService create(String dataDir) {
        Options options = new Options().setCreateIfMissing(true);
        try {
            return lockAndCreateDb(options, new File(dataDir));
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * A thread-safer mkdirs. mkdirs() will fail if another thread creates one of the directories it means to create
     * before it does. http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4742723
     *
     * This attempts the mkdirs() multiple times, so that if another thread does create one of the directories,
     * the next call to mkdirs() will take that into account.
     *
     * Note: this returns true iff the file f ends up being a directory, contrary to mkdirs(), which returns false
     * if it existed before the call.
     */
    // This was copied from FileUtils so we don't have to depend on commons
    private static boolean mkdirsWithRetry(final File f) {
        if (f.exists()) {
            return f.isDirectory();
        }
        for (int i = 0; i < 10 && !f.isDirectory(); ++i) {
            f.mkdirs();
        }
        return f.isDirectory();
    }

    private static RocksDbKeyValueService lockAndCreateDb(Options options, File dbDir) throws IOException, RocksDBException {
        mkdirsWithRetry(dbDir);
        Preconditions.checkArgument(dbDir.exists() && dbDir.isDirectory(), "DB file must be a directory: " + dbDir);
        final RandomAccessFile randomAccessFile =
            new RandomAccessFile(
                /* NB: We cannot write files into the LevelDB database directory. Upon opening a database, LevelDB
                       audits the files present in the directory, and if it runs into any that don't conform to its
                       expected name patterns, it fails with a NullPointerException. Rather than trying to trick it into
                       accepting our lock file name as valid, write the file as a sibling to the database directory,
                       constructing its name to include the name of the database directory as a suffix.

                   NB: The documentation for File#deleteOnExit() advises against using it in concert with file locking,
                       so we'll allow this file to remain in place after the program exits.
                       See http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4676183
                 */
                new File(dbDir.getParentFile(),
                         String.format("%s_%s", LOCK_FILE_PREFIX, dbDir.getName())),
                "rws");
        final FileChannel channel = randomAccessFile.getChannel();
        boolean success = false;
        try {
            final FileLock lock = channel.tryLock();
            if (lock == null) {
                throw new IOException("Cannot lock. Someone already has this database open: " + dbDir);
            }
            List<byte[]> initialCfs = MoreObjects.firstNonNull(
                    RocksDB.listColumnFamilies(options, dbDir.getAbsolutePath()), ImmutableList.<byte[]>of());
            List<ColumnFamilyDescriptor> cfDescriptors = Lists.newArrayListWithCapacity(initialCfs.size());
            List<ColumnFamilyHandle> cfHandles = Lists.newArrayListWithCapacity(1 + initialCfs.size());
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
            for (byte[] cf : initialCfs) {
                cfDescriptors.add(new ColumnFamilyDescriptor(cf, getCfOptions(options, new String(cf, Charsets.UTF_8))));
            }
            RocksDB db = RocksDB.open(getDBOptions(options), dbDir.getAbsolutePath(), cfDescriptors, cfHandles);
            Preconditions.checkState(cfDescriptors.size() == cfHandles.size());
            ConcurrentMap<String, ColumnFamilyHandle> tables = Maps.newConcurrentMap();
            for (int i = 0; i < cfDescriptors.size(); i++) {
                tables.put(new String(cfDescriptors.get(i).columnFamilyName(), Charsets.UTF_8), cfHandles.get(i));
            }
            RocksDbKeyValueService ret = new RocksDbKeyValueService(db, options, tables, lock, randomAccessFile);
            ret.createTable(METADATA_TABLE_NAME, Integer.MAX_VALUE);
            success = true;
            return ret;
        } catch (OverlappingFileLockException e) {
            throw new IOException("Cannot lock. This jvm already has this database open: " + dbDir);
        } finally {
            if (!success) {
                randomAccessFile.close();
            }
        }
    }

    private static DBOptions getDBOptions(Options options) {
        DBOptions dbOptions = new DBOptions();
        dbOptions.setCreateIfMissing(options.createIfMissing());
        return dbOptions;
    }

    private static ColumnFamilyOptions getCfOptions(Options options,
                                                    String tableName) {
        ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
        if (!tableName.equals(METADATA_TABLE_NAME)) {
            cfOptions.setComparator(RocksComparator.INSTANCE);
        }
        return cfOptions;
    }

    private RocksDbKeyValueService(RocksDB db,
                                   Options options,
                                   ConcurrentMap<String, ColumnFamilyHandle> tables,
                                   FileLock lock,
                                   RandomAccessFile file) {
        this.db = db;
        this.options = options;
        this.tables = tables;
        this.lock = lock;
        this.lockFile = file;
    }

    @Override
    public void initializeFromFreshInstance() {
        // nothing
    }

    @Override
    public void close() {
        if (!closed) {
            try {
                getDb().close();
                lock.release();
                lockFile.close();
            } catch (IOException e) {
                throw Throwables.propagate(e);
            } finally {
                closed = true;
            }
        }
    }

    @Override
    public void teardown() {
        close();
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableList.of();
    }

    @Override
    public Map<Cell, Value> getRows(String tableName,
                                    Iterable<byte[]> rows,
                                    ColumnSelection columnSelection,
                                    long timestamp) {
        Map<Cell, Value> results = Maps.newHashMap();
        RocksIterator iter = getDb().newIterator(getTable(tableName));
        try {
            for (byte[] row : rows) {
                RocksDbKeyValueServices.getRow(iter, row, columnSelection, timestamp, results);
            }
        } finally {
            iter.dispose();
        }
        return results;
    }

    @Override
    public Map<Cell, Value> get(String tableName,
                                Map<Cell, Long> timestampByCell) {
        Map<Cell, Value> results = Maps.newHashMap();
        RocksIterator iter = getDb().newIterator(getTable(tableName));
        try {
            for (Entry<Cell, Long> entry : timestampByCell.entrySet()) {
                Value value = RocksDbKeyValueServices.getCell(iter, entry.getKey(), entry.getValue());
                if (value != null) {
                    results.put(entry.getKey(), value);
                }
            }
        } finally {
            iter.dispose();
        }
        return results;
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(String tableName,
                                               Map<Cell, Long> timestampByCell) {
        Map<Cell, Long> results = Maps.newHashMap();
        RocksIterator iter = getDb().newIterator(getTable(tableName));
        try {
            for (Entry<Cell, Long> entry : timestampByCell.entrySet()) {
                Long ts = RocksDbKeyValueServices.getTimestamp(iter, entry.getKey(), entry.getValue());
                if (ts != null) {
                    results.put(entry.getKey(), ts);
                }
            }
        } finally {
            iter.dispose();
        }
        return results;
    }

    @Override
    public void put(String tableName, Map<Cell, byte[]> values, long timestamp) {
        ColumnFamilyHandle table = getTable(tableName);
        try (Disposer d = new Disposer()) {
            WriteOptions options = d.register(new WriteOptions().setSync(true));
            WriteBatch batch = d.register(new WriteBatch());
            for (Entry<Cell, byte[]> entry : values.entrySet()) {
                byte[] key = RocksDbKeyValueServices.getKey(entry.getKey(), timestamp);
                batch.put(table, key, entry.getValue());
            }
            getDb().write(options, batch);
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        try (Disposer d = new Disposer()) {
            WriteOptions options = d.register(new WriteOptions().setSync(true));
            WriteBatch batch = d.register(new WriteBatch());
            for (Entry<String, ? extends Map<Cell, byte[]>> entry : valuesByTable.entrySet()) {
                ColumnFamilyHandle table = getTable(entry.getKey());
                for (Entry<Cell, byte[]> subEntry : entry.getValue().entrySet()) {
                    byte[] key = RocksDbKeyValueServices.getKey(subEntry.getKey(), timestamp);
                    batch.put(table, key, subEntry.getValue());
                }
            }
            getDb().write(options, batch);
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> cellValues) {
        ColumnFamilyHandle table = getTable(tableName);
        try (Disposer d = new Disposer()) {
            WriteOptions options = d.register(new WriteOptions().setSync(true));
            WriteBatch batch = d.register(new WriteBatch());
            for (Entry<Cell, Value> entry : cellValues.entries()) {
                Value value = entry.getValue();
                byte[] key = RocksDbKeyValueServices.getKey(entry.getKey(), value.getTimestamp());
                batch.put(table, key, value.getContents());
            }
            getDb().write(options, batch);
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void putUnlessExists(String tableName, Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        ColumnFamilyHandle table = getTable(tableName);
        LockState<Cell> locks = lockSet.lockOnObjects(values.keySet());
        try (Disposer d = new Disposer()) {
            Set<Cell> alreadyExists = Sets.newHashSetWithExpectedSize(0);
            WriteOptions options = d.register(new WriteOptions().setSync(true));
            WriteBatch batch = d.register(new WriteBatch());
            RocksIterator iter = d.register(getDb().newIterator(getTable(tableName)));
            for (Entry<Cell, byte[]> entry : values.entrySet()) {
                byte[] key = RocksDbKeyValueServices.getKey(entry.getKey(), PUT_UNLESS_EXISTS_TS);
                if (RocksDbKeyValueServices.keyExists(iter, key)) {
                    alreadyExists.add(entry.getKey());
                } else {
                    batch.put(table, key, entry.getValue());
                }
            }
            getDb().write(options, batch);
            if (!alreadyExists.isEmpty()) {
                throw new KeyAlreadyExistsException("key already exists", alreadyExists);
            }
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        } finally {
            locks.unlock();
        }
    }

    @Override
    public void delete(String tableName, Multimap<Cell, Long> keys) {
        ColumnFamilyHandle table = getTable(tableName);
        try (Disposer d = new Disposer()) {
            WriteOptions options = d.register(new WriteOptions().setSync(true));
            WriteBatch batch = d.register(new WriteBatch());
            for (Entry<Cell, Long> entry : keys.entries()) {
                byte[] key = RocksDbKeyValueServices.getKey(entry.getKey(), entry.getValue());
                batch.remove(table, key);
            }
            getDb().write(options, batch);
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void truncateTable(String tableName) {
        dropTable(tableName);
        createTable(tableName, Integer.MAX_VALUE);
    }

    @Override
    public void truncateTables(Set<String> tableNames) {
        for (String tableName : tableNames) {
            truncateTable(tableName);
        }
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(String tableName,
                                                       RangeRequest rangeRequest,
                                                       long timestamp) {
        ColumnFamilyHandle table = getTable(tableName);
        RocksIterator iter = getDb().newIterator(table);
        return new ValueRangeIterator(iter, rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        ColumnFamilyHandle table = getTable(tableName);
        RocksIterator iter = getDb().newIterator(table);
        return new HistoryRangeIterator(iter, rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        ColumnFamilyHandle table = getTable(tableName);
        RocksIterator iter = getDb().newIterator(table);
        return new TimestampRangeIterator(iter, rangeRequest, timestamp);
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName,
                                                                                                           Iterable<RangeRequest> rangeRequests,
                                                                                                           long timestamp) {
        return KeyValueServices.getFirstBatchForRangesUsingGetRange(this, tableName, rangeRequests, timestamp);
    }

    @Override
    public void dropTable(String tableName) {
        ColumnFamilyHandle table;
        try {
            table = getTable(tableName);
        } catch (IllegalArgumentException e) {
            // ignore, table didn't exist
            return;
        }
        try {
            if (tables.remove(tableName, table)) {
                getDb().dropColumnFamily(table);
                table.dispose();
            }
        } catch (IllegalArgumentException e) {
            // ignore, table didn't exist
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void dropTables(Set<String> tableNames) throws InsufficientConsistencyException {
        for (String tableName : tableNames) {
            dropTable(tableName);
        }
    }

    @Override
    public void createTable(String tableName, int maxValueSizeInBytes) {
        createTables(ImmutableMap.of(tableName, maxValueSizeInBytes));
    }

    @Override
    public void createTables(Map<String, Integer> tableNamesToMaxValueSizeInBytes)
            throws InsufficientConsistencyException {
        for (String tableName : tableNamesToMaxValueSizeInBytes.keySet()) {
            if (tables.containsKey(tableName)) {
                return;
            }
            try {
                ColumnFamilyDescriptor descriptor = new ColumnFamilyDescriptor(
                        tableName.getBytes(Charsets.UTF_8), getCfOptions(options, tableName));
                ColumnFamilyHandle handle = getDb().createColumnFamily(descriptor);
                if (tables.putIfAbsent(tableName, handle) != null) {
                    handle.dispose();
                }
            } catch (RocksDBException e) {
                // ignore
            }
        }
        putMetadataForTables(Maps2.createConstantValueMap(tableNamesToMaxValueSizeInBytes.keySet(), new byte[0]));
    }

    @Override
    public Set<String> getAllTableNames() {
        return Sets.difference(tables.keySet(), ImmutableSet.of(METADATA_TABLE_NAME,
                new String(RocksDB.DEFAULT_COLUMN_FAMILY, Charsets.UTF_8)));
    }

    @Override
    public byte[] getMetadataForTable(String tableName) {
        ColumnFamilyHandle metadataTable = getTable(METADATA_TABLE_NAME);
        try {
            return getDb().get(metadataTable, tableName.getBytes(Charsets.UTF_8));
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Map<String, byte[]> getMetadataForTables() {
        ColumnFamilyHandle metadataTable = getTable(METADATA_TABLE_NAME);
        Set<String> tableNames = tables.keySet();
        List<ColumnFamilyHandle> tables = Lists.newArrayListWithCapacity(tableNames.size());
        List<byte[]> keys = Lists.newArrayListWithCapacity(tableNames.size());
        for (String tableName : tableNames) {
            tables.add(metadataTable);
            keys.add(tableName.getBytes(Charsets.UTF_8));
        }
        try {
            Map<byte[], byte[]> rawResults = getDb().multiGet(tables, keys);
            Map<String, byte[]> results = Maps.newHashMapWithExpectedSize(rawResults.size());
            for (Entry<byte[], byte[]> entry : rawResults.entrySet()) {
                results.put(new String(entry.getKey(), Charsets.UTF_8), entry.getValue());
            }
            return results;
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void putMetadataForTable(String tableName, byte[] metadata) {
        ColumnFamilyHandle metadataTable = getTable(METADATA_TABLE_NAME);
        WriteOptions options = new WriteOptions().setSync(true);
        try {
            getDb().put(metadataTable, options, tableName.getBytes(Charsets.UTF_8), metadata);
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        } finally {
            options.dispose();
        }
    }

    @Override
    public void putMetadataForTables(Map<String, byte[]> tableNameToMetadata) {
        try (Disposer d = new Disposer()) {
            ColumnFamilyHandle metadataTable = getTable(METADATA_TABLE_NAME);
            WriteOptions options = d.register(new WriteOptions().setSync(true));
            WriteBatch batch = d.register(new WriteBatch());
            for (Entry<String, byte[]> entry : tableNameToMetadata.entrySet()) {
                batch.put(metadataTable, entry.getKey().getBytes(Charsets.UTF_8), entry.getValue());
            }
            getDb().write(options, batch);
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void addGarbageCollectionSentinelValues(String tableName,
                                                   Set<Cell> cells) {
        ColumnFamilyHandle table = getTable(tableName);
        byte[] val = new byte[0];
        try (Disposer d = new Disposer()) {
            WriteOptions options = d.register(new WriteOptions().setSync(true));
            WriteBatch batch = d.register(new WriteBatch());
            for (Cell cell : cells) {
                byte[] key = RocksDbKeyValueServices.getKey(cell, Value.INVALID_VALUE_TIMESTAMP);
                batch.put(table, key, val);
            }
            getDb().write(options, batch);
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(String tableName,
                                                 Set<Cell> cells,
                                                 long timestamp) {
        ColumnFamilyHandle table = getTable(tableName);
        Multimap<Cell, Long> results = ArrayListMultimap.create();
        RocksIterator iter = getDb().newIterator(table);
        try {
            for (Cell cell : cells) {
                RocksDbKeyValueServices.getTimestamps(iter, cell, timestamp, results);
            }
        } finally {
            iter.dispose();
        }
        return results;
    }

    @Override
    public void compactInternally(String tableName) {
        // nothing
    }

    private ColumnFamilyHandle getTable(String tableName) {
        ColumnFamilyHandle handle = tables.get(tableName);
        Preconditions.checkArgument(handle != null, "Table %s does not exist.", tableName);
        return handle;
    }

    private RocksDB getDb() {
        if (closed) {
            throw new IllegalStateException("Database has been closed.");
        }
        return db;
    }
}
