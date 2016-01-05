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
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
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
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.atlasdb.keyvalue.rocksdb.impl.ColumnFamilyMap.ColumnFamily;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.MutuallyExclusiveSetLock;
import com.palantir.util.MutuallyExclusiveSetLock.LockState;
import com.palantir.util.file.TempFileUtils;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class RocksDbKeyValueService implements KeyValueService {
    private static final Logger log = LoggerFactory.getLogger(RocksDbKeyValueService.class);
    private static final String METADATA_TABLE_NAME = "_metadata";
    private static final long PUT_UNLESS_EXISTS_TS = 0L;
    private static final String LOCK_FILE_PREFIX = ".pt_kv_lock";
    final RocksDB db;
    final ColumnFamilyMap columnFamilies;
    private final FileLock lock;
    private final RandomAccessFile lockFile;
    private final WriteOpts writeOptions;
    private final MutuallyExclusiveSetLock<Cell> lockSet = MutuallyExclusiveSetLock.<Cell>create(false);
    private volatile boolean closed = false;

    public static RocksDbKeyValueService create(String dataDir) {
        return create(dataDir,
                ImmutableMap.<String, String>of(),
                ImmutableMap.<String, String>of(),
                ImmutableWriteOpts.builder().build(),
                RocksComparatorName.V2.getComparatorName());
    }

    public static RocksDbKeyValueService create(String dataDir,
                                                Map<String, String> dbOptions,
                                                Map<String, String> cfOptions,
                                                WriteOpts writeOpts,
                                                String comparator) {
        DBOptions dbOpts = new DBOptions().setCreateIfMissing(true);
        setReflectionOpts(dbOpts, dbOptions);
        ColumnFamilyOptions cfMetadataOpts = new ColumnFamilyOptions();
        setReflectionOpts(cfMetadataOpts, cfOptions);
        ColumnFamilyOptions cfCommonOpts;
        switch (comparator) {
        case "atlasdb-v2":
            cfCommonOpts = new ColumnFamilyOptions().setComparator(RocksComparator.INSTANCE);
            break;
        case "atlasdb":
            cfCommonOpts = new ColumnFamilyOptions().setComparator(RocksOldComparator.INSTANCE);
            break;
        default:
            throw new IllegalArgumentException("Unknown comparator " + comparator);
        }
        setReflectionOpts(cfCommonOpts, cfOptions);
        return create(dataDir, dbOpts, cfMetadataOpts, cfCommonOpts, writeOpts);
    }

    private static void setReflectionOpts(Object opts,
                                          Map<String, String> stringOpts) {
        Method[] methods = opts.getClass().getMethods();
        for (Method method : methods) {
            String methodName = method.getName();
            Class<?>[] params = method.getParameterTypes();
            if (methodName.startsWith("set") && methodName.length() >= 4 && params.length == 1) {
                String optName = Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4);
                String value = stringOpts.get(optName);
                if (value != null) {
                    try {
                        if (params[0] == boolean.class) {
                            method.invoke(opts, Boolean.parseBoolean(value));
                        } else if (params[0] == int.class) {
                            method.invoke(opts, Integer.parseInt(value));
                        } else if (params[0] == long.class) {
                            method.invoke(opts, Long.parseLong(value));
                        } else if (params[0] == double.class) {
                            method.invoke(opts, Double.parseDouble(value));
                        } else if (params[0] == String.class) {
                            method.invoke(opts, value);
                        }
                    } catch (Exception e) {
                        throw Throwables.propagate(e);
                    }
                }
            }
        }
    }

    public static RocksDbKeyValueService create(String dataDir,
                                                DBOptions dbOptions,
                                                ColumnFamilyOptions cfMetadataOptions,
                                                ColumnFamilyOptions cfCommonOptions,
                                                WriteOpts writeOptions) {
        try {
            RocksDbKeyValueService kvs = lockAndCreateDb(new File(dataDir), dbOptions, cfMetadataOptions, cfCommonOptions, writeOptions);
            registerMBean(kvs);
            return kvs;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private static void registerMBean(RocksDbKeyValueService kvs) {
        try {
            RocksDbMXBean mbean = new RocksDbMXBeanImpl(kvs.db, kvs.columnFamilies);
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName("com.palantir.rocksdb:type=RocksDbMBean,name=" + System.identityHashCode(mbean));
            mbs.registerMBean(mbean, name);
        } catch (Exception e) {
            log.error("Failed to register mbean for rocksdb. It will " +
                    "not be possible to force compactions or view stats through jmx.", e);
        }
    }

    private static RocksDbKeyValueService lockAndCreateDb(File dbDir,
                                                          final DBOptions dbOptions,
                                                          final ColumnFamilyOptions cfMetadataOptions,
                                                          final ColumnFamilyOptions cfCommonOptions,
                                                          final WriteOpts writeOpts) throws IOException, RocksDBException {
        TempFileUtils.mkdirsWithRetry(dbDir);
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
                    RocksDB.listColumnFamilies(new Options(dbOptions, cfMetadataOptions), dbDir.getAbsolutePath()), ImmutableList.<byte[]>of());
            List<ColumnFamilyDescriptor> cfDescriptors = Lists.newArrayListWithCapacity(initialCfs.size());
            List<ColumnFamilyHandle> cfHandles = Lists.newArrayListWithCapacity(1 + initialCfs.size());
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
            for (byte[] cf : initialCfs) {
                String tableName = new String(cf, Charsets.UTF_8);
                cfDescriptors.add(getCfDescriptor(tableName, cfMetadataOptions, cfCommonOptions));
            }
            RocksDB db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), cfDescriptors, cfHandles);
            Preconditions.checkState(cfDescriptors.size() == cfHandles.size());
            ColumnFamilyMap columnFamilies = new ColumnFamilyMap(new Function<String, ColumnFamilyDescriptor>() {
                @Override
                public ColumnFamilyDescriptor apply(String tableName) {
                    return getCfDescriptor(tableName, cfMetadataOptions, cfCommonOptions);
                }
            }, db);
            columnFamilies.initialize(cfDescriptors, cfHandles);
            RocksDbKeyValueService ret = new RocksDbKeyValueService(db, columnFamilies, lock, randomAccessFile, writeOpts);
            ret.createTable(METADATA_TABLE_NAME, AtlasDbConstants.EMPTY_TABLE_METADATA);
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

    private static ColumnFamilyDescriptor getCfDescriptor(String tableName,
                                                          ColumnFamilyOptions cfMetadataOptions,
                                                          ColumnFamilyOptions cfCommonOptions) {
        if (tableName.equals(METADATA_TABLE_NAME)) {
            return new ColumnFamilyDescriptor(tableName.getBytes(Charsets.UTF_8), cfMetadataOptions);
        } else {
            return new ColumnFamilyDescriptor(tableName.getBytes(Charsets.UTF_8), cfCommonOptions);
        }
    }

    private RocksDbKeyValueService(RocksDB db,
                                   ColumnFamilyMap columnFamilies,
                                   FileLock lock,
                                   RandomAccessFile file,
                                   WriteOpts writeOptions) {
        this.db = db;
        this.columnFamilies = columnFamilies;
        this.lock = lock;
        this.lockFile = file;
        this.writeOptions = writeOptions;
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
        try (Disposer d = new Disposer();
                ColumnFamily table = columnFamilies.get(tableName)) {
            Map<Cell, Value> results = Maps.newHashMap();
            RocksIterator iter = d.register(getDb().newIterator(table.getHandle()));
            for (byte[] row : rows) {
                RocksDbKeyValueServices.getRow(iter, row, columnSelection, timestamp, results);
            }
            return results;
        }
    }

    @Override
    public Map<Cell, Value> get(String tableName,
                                Map<Cell, Long> timestampByCell) {
        try (Disposer d = new Disposer();
                ColumnFamily table = columnFamilies.get(tableName)) {
            Map<Cell, Value> results = Maps.newHashMap();
            RocksIterator iter = d.register(getDb().newIterator(table.getHandle()));
            for (Entry<Cell, Long> entry : timestampByCell.entrySet()) {
                Value value = RocksDbKeyValueServices.getCell(iter, entry.getKey(), entry.getValue());
                if (value != null) {
                    results.put(entry.getKey(), value);
                }
            }
            return results;
        }
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(String tableName,
                                               Map<Cell, Long> timestampByCell) {
        try (Disposer d = new Disposer();
                ColumnFamily table = columnFamilies.get(tableName)) {
            Map<Cell, Long> results = Maps.newHashMap();
            RocksIterator iter = d.register(getDb().newIterator(table.getHandle()));
            for (Entry<Cell, Long> entry : timestampByCell.entrySet()) {
                Long ts = RocksDbKeyValueServices.getTimestamp(iter, entry.getKey(), entry.getValue());
                if (ts != null) {
                    results.put(entry.getKey(), ts);
                }
            }
            return results;
        }
    }

    @Override
    public void put(String tableName, Map<Cell, byte[]> values, long timestamp) {
        try (Disposer d = new Disposer();
                ColumnFamily table = columnFamilies.get(tableName)) {
            WriteOptions options = d.register(new WriteOptions().setSync(writeOptions.fsyncPut()));
            WriteBatch batch = d.register(new WriteBatch());
            for (Entry<Cell, byte[]> entry : values.entrySet()) {
                byte[] key = RocksDbKeyValueServices.getKey(entry.getKey(), timestamp);
                batch.put(table.getHandle(), key, entry.getValue());
            }
            getDb().write(options, batch);
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        Map<String, ColumnFamily> cfs = Maps.newHashMapWithExpectedSize(valuesByTable.size());
        try {
            for (String tableName : valuesByTable.keySet()) {
                cfs.put(tableName, columnFamilies.get(tableName));
            }
            try (Disposer d = new Disposer()) {
                WriteOptions options = d.register(new WriteOptions().setSync(writeOptions.fsyncPut()));
                WriteBatch batch = d.register(new WriteBatch());
                for (Entry<String, ? extends Map<Cell, byte[]>> entry : valuesByTable.entrySet()) {
                    ColumnFamilyHandle table = cfs.get(entry.getKey()).getHandle();
                    for (Entry<Cell, byte[]> subEntry : entry.getValue().entrySet()) {
                        byte[] key = RocksDbKeyValueServices.getKey(subEntry.getKey(), timestamp);
                        batch.put(table, key, subEntry.getValue());
                    }
                }
                getDb().write(options, batch);
            } catch (RocksDBException e) {
                throw Throwables.propagate(e);
            }
        } finally {
            for (ColumnFamily cf : cfs.values()) {
                cf.close();
            }
        }
    }

    @Override
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> cellValues) {
        try (Disposer d = new Disposer();
                ColumnFamily table = columnFamilies.get(tableName)) {
            WriteOptions options = d.register(new WriteOptions().setSync(writeOptions.fsyncPut()));
            WriteBatch batch = d.register(new WriteBatch());
            for (Entry<Cell, Value> entry : cellValues.entries()) {
                Value value = entry.getValue();
                byte[] key = RocksDbKeyValueServices.getKey(entry.getKey(), value.getTimestamp());
                batch.put(table.getHandle(), key, value.getContents());
            }
            getDb().write(options, batch);
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void putUnlessExists(String tableName, Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        LockState<Cell> locks = lockSet.lockOnObjects(values.keySet());
        try (Disposer d = new Disposer();
                ColumnFamily table = columnFamilies.get(tableName)) {
            Set<Cell> alreadyExists = Sets.newHashSetWithExpectedSize(0);
            WriteOptions options = d.register(new WriteOptions().setSync(writeOptions.fsyncCommit()));
            WriteBatch batch = d.register(new WriteBatch());
            RocksIterator iter = d.register(getDb().newIterator(table.getHandle()));
            for (Entry<Cell, byte[]> entry : values.entrySet()) {
                byte[] key = RocksDbKeyValueServices.getKey(entry.getKey(), PUT_UNLESS_EXISTS_TS);
                if (RocksDbKeyValueServices.keyExists(iter, key)) {
                    alreadyExists.add(entry.getKey());
                } else {
                    batch.put(table.getHandle(), key, entry.getValue());
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
        try (Disposer d = new Disposer();
                ColumnFamily table = columnFamilies.get(tableName)) {
            WriteOptions options = d.register(new WriteOptions().setSync(writeOptions.fsyncPut()));
            WriteBatch batch = d.register(new WriteBatch());
            for (Entry<Cell, Long> entry : keys.entries()) {
                byte[] key = RocksDbKeyValueServices.getKey(entry.getKey(), entry.getValue());
                batch.remove(table.getHandle(), key);
            }
            getDb().write(options, batch);
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void truncateTable(String tableName) {
        try {
            columnFamilies.truncate(tableName);
        } catch (RocksDBException | InterruptedException e) {
            throw Throwables.propagate(e);
        }
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
        ColumnFamily table = columnFamilies.get(tableName);
        RocksIterator iter = getDb().newIterator(table.getHandle());
        return new ValueRangeIterator(table, iter, rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        ColumnFamily table = columnFamilies.get(tableName);
        RocksIterator iter = getDb().newIterator(table.getHandle());
        return new HistoryRangeIterator(table, iter, rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        ColumnFamily table = columnFamilies.get(tableName);
        RocksIterator iter = getDb().newIterator(table.getHandle());
        return new TimestampRangeIterator(table, iter, rangeRequest, timestamp);
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName,
                                                                                                           Iterable<RangeRequest> rangeRequests,
                                                                                                           long timestamp) {
        return KeyValueServices.getFirstBatchForRangesUsingGetRange(this, tableName, rangeRequests, timestamp);
    }

    @Override
    public void dropTable(String tableName) {
        try {
            columnFamilies.drop(tableName);
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
    public void createTable(String tableName, byte[] tableMetadata) {
        createTables(ImmutableMap.of(tableName, tableMetadata));
    }

    @Override
    public void createTables(Map<String, byte[]> tableNameToTableMetadata)
            throws InsufficientConsistencyException {
        for (String tableName : tableNameToTableMetadata.keySet()) {
            try {
                columnFamilies.create(tableName);
            } catch (RocksDBException e) {
                Throwables.propagate(e);
            }
        }
        putMetadataForTables(tableNameToTableMetadata);
    }

    @Override
    public Set<String> getAllTableNames() {
        Set<String> hiddenTables = ImmutableSet.of(
                METADATA_TABLE_NAME,
                new String(RocksDB.DEFAULT_COLUMN_FAMILY, Charsets.UTF_8),
                AtlasDbConstants.TIMESTAMP_TABLE);
        return Sets.difference(columnFamilies.getTableNames(), hiddenTables);
    }

    @Override
    public byte[] getMetadataForTable(String tableName) {
        try (ColumnFamily metadataTable = columnFamilies.get(METADATA_TABLE_NAME)) {
            return getDb().get(metadataTable.getHandle(), tableName.getBytes(Charsets.UTF_8));
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Map<String, byte[]> getMetadataForTables() {
        Set<String> tableNames = columnFamilies.getTableNames();
        List<ColumnFamilyHandle> tables = Lists.newArrayListWithCapacity(tableNames.size());
        List<byte[]> keys = Lists.newArrayListWithCapacity(tableNames.size());
        try (ColumnFamily metadataTable = columnFamilies.get(METADATA_TABLE_NAME)) {
            for (String tableName : tableNames) {
                tables.add(metadataTable.getHandle());
                keys.add(tableName.getBytes(Charsets.UTF_8));
            }
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
        try (Disposer d = new Disposer();
                ColumnFamily metadataTable = columnFamilies.get(METADATA_TABLE_NAME)) {
            WriteOptions options = d.register(new WriteOptions().setSync(true));
            getDb().put(metadataTable.getHandle(), options, tableName.getBytes(Charsets.UTF_8), metadata);
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void putMetadataForTables(Map<String, byte[]> tableNameToMetadata) {
        try (Disposer d = new Disposer();
                ColumnFamily metadataTable = columnFamilies.get(METADATA_TABLE_NAME)) {
            WriteOptions options = d.register(new WriteOptions().setSync(true));
            WriteBatch batch = d.register(new WriteBatch());
            for (Entry<String, byte[]> entry : tableNameToMetadata.entrySet()) {
                batch.put(metadataTable.getHandle(), entry.getKey().getBytes(Charsets.UTF_8), entry.getValue());
            }
            getDb().write(options, batch);
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void addGarbageCollectionSentinelValues(String tableName,
                                                   Set<Cell> cells) {
        try (ColumnFamily table = columnFamilies.get(tableName)) {
            byte[] val = new byte[0];
            try (Disposer d = new Disposer()) {
                WriteOptions options = d.register(new WriteOptions().setSync(true));
                WriteBatch batch = d.register(new WriteBatch());
                for (Cell cell : cells) {
                    byte[] key = RocksDbKeyValueServices.getKey(cell, Value.INVALID_VALUE_TIMESTAMP);
                    batch.put(table.getHandle(), key, val);
                }
                getDb().write(options, batch);
            } catch (RocksDBException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(String tableName,
                                                 Set<Cell> cells,
                                                 long timestamp) {
        try (ColumnFamily table = columnFamilies.get(tableName)) {
            Multimap<Cell, Long> results = ArrayListMultimap.create();
            RocksIterator iter = getDb().newIterator(table.getHandle());
            try {
                for (Cell cell : cells) {
                    RocksDbKeyValueServices.getTimestamps(iter, cell, timestamp, results);
                }
            } finally {
                iter.dispose();
            }
            return results;
        }
    }

    @Override
    public void compactInternally(String tableName) {
        // nothing
    }

    private RocksDB getDb() {
        if (closed) {
            throw new IllegalStateException("Database has been closed.");
        }
        return db;
    }
}
