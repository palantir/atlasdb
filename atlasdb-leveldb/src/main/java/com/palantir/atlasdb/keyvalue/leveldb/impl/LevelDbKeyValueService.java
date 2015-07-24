// Copyright 2015 Palantir Technologies
//
// Licensed under the BSD-3 License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.keyvalue.leveldb.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.Validate;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.Throwables;
import com.palantir.util.MutuallyExclusiveSetLock;
import com.palantir.util.MutuallyExclusiveSetLock.LockState;
import com.palantir.util.Pair;
import com.palantir.util.paging.TokenBackedBasicResultsPage;


public final class LevelDbKeyValueService implements KeyValueService {
    private static final Logger log = LoggerFactory.getLogger(LevelDbKeyValueService.class);

    public static final long TRANSACTION_TS = 0;

    private static final String META_TABLE = "-METADATA_TABLE-";
    private static final byte[] METADATA_COL = PtBytes.toBytes("m");
    private static final byte[] SIZE_COL = PtBytes.toBytes("s");
    private static final byte[] ENCODED_COL = PtBytes.toBytes("e");
    private static final byte[] COMMIT_TS_COLUMN = PtBytes.toBytes("t");
    private static final int MAX_SMALL_TABLE_SIZE = 8;
    private static final String LOCK_FILE_PREFIX = ".pt_kv_lock";
    private static final String TRANSACTION_TABLE = TransactionConstants.TRANSACTION_TABLE;

    private final DB db;
    private final FileLock lock;
    private final RandomAccessFile lockFile;
    private final MutuallyExclusiveSetLock<Cell> lockSet = MutuallyExclusiveSetLock.<Cell>create(false);
    private final Set<String> fastTables = Sets.newSetFromMap(Maps.<String, Boolean>newConcurrentMap());
    private final Map<String, byte[]> encodedName = Maps.newConcurrentMap();


    /**
     * @param file The root directory of the levelDb to use as a key value store
     * @throws DBException if the directory passed isn't a valid database
     * @throws IOException if there is a file locking error (someone already has this db open).
     * Or if there is an ioExcpetion while opening the DB
     */
    public static LevelDbKeyValueService create(File file) throws DBException, IOException {
        final Options options = new Options();
        options.createIfMissing(true);
        return create(options, file);
    }


    /**
     * Options can be specified to control block size, cache, and write buffer size.
     * <p>
     * cache size defaults to 8M and is only used for "small" tables. Cache is only populated on
     * reads.
     * <p>
     * block size it used to control how much data is fetched off of disk for each read. If you are
     * using fusion io, you might want to reduce block size (defaults to 4K)
     * <p>
     * write buffer defaults to 4M.  This is the amount of data in the log and not in SSTables.
     * Once the log hits the write buffer it is compacted to an SSTable.  All of this data also
     * remains in memory.
     *
     * @param file The root directory of the levelDb to use as a key value store
     * @param options The levelDb options to use to open the Db.
     * @throws DBException if the directory passed isn't a valid database
     * @throws IOException if there is a file locking error (someone already has this db open).
     * Or if there is an ioExcpetion while opening the DB
     * @see <a href="https://leveldb.googlecode.com/svn/trunk/doc/index.html">Google project documentation</a>
     */
    public static LevelDbKeyValueService create(Options options, File file) throws DBException, IOException {
        return lockAndCreateDb(options, file);
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


    private static LevelDbKeyValueService lockAndCreateDb(Options options, File dbDir) throws DBException, IOException {
        mkdirsWithRetry(dbDir);
        Validate.isTrue(dbDir.exists() && dbDir.isDirectory(), "DB file must be a directory: " + dbDir);
        final RandomAccessFile randomAccessFile =
            new RandomAccessFile(
                /* NB: We cannot write files into the LevelDB database directory. Upon opening a database, LevelDB
                       audits the files present in the directory, and if it runs into any that don't conform to its
                       expected name patterns, it fails with a NullPointerException. Rather than trying to trick it into
                       accepting our lock file name as valid, write the file as a sibling to the database directory,
                       constructing its name to include the name of the database directory as a suffix.

                   NB: The documentation for File#deleteOnExit() advises against using it in concert with file locking,
                       so we'll allow this file to remain in place after the program exits.
                       See http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4676183.
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
            options.comparator(LevelDbKeyValueUtils.getComparator());
            final DB db = getDBFactory().open(dbDir, options);
            final LevelDbKeyValueService ret = new LevelDbKeyValueService(db, lock, randomAccessFile);
            final Map<String, Integer> allTablesAndSizes = ret.getAllTablesAndSizes();
            for (Map.Entry<String, Integer> e : allTablesAndSizes.entrySet()) {
                if (e.getValue() <= MAX_SMALL_TABLE_SIZE) {
                    ret.fastTables.add(e.getKey());
                }
            }
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

    private LevelDbKeyValueService(DB db, FileLock lock, RandomAccessFile lockFile) {
        this.db = db;
        this.lock = lock;
        this.lockFile = lockFile;
    }


    private static void validateTableName(String tableName) {
        Validate.isTrue(!tableName.isEmpty());
        for (int i = 0, length = tableName.length(); i != length; ++i) {
            final char c = tableName.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '_') {
                throw new IllegalArgumentException("illegal table name: " + tableName);
            }
        }
    }


    @Override
    public void close() {
        try {
            db.close();
            lock.release();
            lockFile.close();
        } catch (IOException e) {
            Throwables.throwUncheckedException(e);
        }
    }

    @Override
    @Idempotent
    public Map<Cell, Value> getRows(String tableName,
                             Iterable<byte[]> rows,
                             ColumnSelection columnSelection,
                             long timestamp) {
        final Map<Cell, Value> ret = Maps.newHashMap();
        final byte[] tablePrefix = getKeyPrefixWithTable(tableName);
        for (byte[] rowName : rows) {
            ret.putAll(getRow(tableName, tablePrefix, rowName, timestamp, columnSelection));
        }
        return ret;
    }

    @Override
    public Map<Cell, Value> get(String tableName, Map<Cell, Long> timestampByCell) {
        final byte[] tablePrefix = getKeyPrefixWithTable(tableName);
        final Map<Cell, Value> ret = Maps.newHashMap();
        for (Map.Entry<Cell, Long> e : timestampByCell.entrySet()) {
            final Value value = getCell(tablePrefix, getReadOptionsForTable(tableName), e.getKey(), e.getValue());
            if (value != null) {
                ret.put(e.getKey(), value);
            }
        }
        return ret;
    }


    @Nonnull
    private Map<Cell, Value> getRow(String tableName,
                                    byte[] tablePrefix,
                                    byte[] rowName,
                                    long timestamp,
                                    ColumnSelection columns) {
        final Map<Cell, Value> ret = Maps.newHashMap();
        final byte[] rowPrefix = LevelDbKeyValueUtils.getKeyPrefixWithRow(tablePrefix, rowName);
        final DBIterator it = db.iterator(getReadOptionsForTable(tableName));
        try {
            it.seek(LevelDbKeyValueUtils.getKey(tablePrefix, Cells.createSmallestCellForRow(rowName), 0L));
            for (; it.hasNext() && PtBytes.startsWith(it.peekNext().getKey(), rowPrefix) ; it.next()) {
                final Pair<Cell, Long> cellAndTs =
                    LevelDbKeyValueUtils.parseCellAndTs(it.peekNext().getKey(), tablePrefix);
                if (!Arrays.equals(rowName, cellAndTs.lhSide.getRowName())) {
                    continue;
                }
                final long lastTs = cellAndTs.rhSide;
                if (lastTs < timestamp && columns.contains(cellAndTs.lhSide.getColumnName())) {
                    ret.put(cellAndTs.lhSide, Value.create(it.peekNext().getValue(), lastTs));
                }
            }
            return ret;
        } catch (DBException e) {
            throw Throwables.throwUncheckedException(e);
        } finally {
            try {
                it.close();
            } catch (IOException e) {
                log.warn("close failed", e);
            }
        }
    }


    @Nullable
    private Value getCell(byte[] tablePrefix, ReadOptions options, Cell cell, long timestamp) {
        Value ret = null;
        final byte[] cellPrefix = LevelDbKeyValueUtils.getKeyPrefixWithCell(tablePrefix, cell);
        final DBIterator it = db.iterator(options);
        try {
            it.seek(LevelDbKeyValueUtils.getKey(tablePrefix, cell, 0L));
            for (; it.hasNext() && PtBytes.startsWith(it.peekNext().getKey(), cellPrefix) ; it.next()) {
                final Pair<Cell, Long> cellAndTs =
                    LevelDbKeyValueUtils.parseCellAndTs(it.peekNext().getKey(), tablePrefix);
                if (!cellAndTs.lhSide.equals(cell)) {
                    continue;
                }
                final long lastTs = cellAndTs.rhSide;
                if (lastTs >= timestamp) {
                    return ret;
                }
                ret = Value.create(it.peekNext().getValue(), lastTs);
            }
            return ret;
        } catch (DBException e) {
            throw Throwables.throwUncheckedException(e);
        } finally {
            try {
                it.close();
            } catch (IOException e) {
                log.warn("failed to close", e);
            }
        }
    }


    @Override
    public Map<Cell, Long> getLatestTimestamps(String tableName, Map<Cell, Long> keys) {
        return Maps.transformValues(get(tableName, keys), new Function<Value, Long>() {
            @Override
            public Long apply(Value from) {
                return from.getTimestamp();
            }
        });
    }


    @Override
    public void put(String tableName, Map<Cell, byte[]> values, long timestamp) {
        if (isTransactionTable(tableName)) {
            Validate.isTrue(timestamp == TRANSACTION_TS);
            putTransaction(values);
        } else {
            putInternal(tableName,
                    KeyValueServices.toConstantTimestampValues(values.entrySet(), timestamp),
                    new WriteOptions());
        }
    }

    @Override
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> values) {
        Validate.isTrue(!isTransactionTable(tableName));
        putInternal(tableName, values.entries(), new WriteOptions());
    }

    private void putInternal(String tableName, Collection<Map.Entry<Cell, Value>> values,
                             WriteOptions writeOptions) {
        final WriteBatch batch = createWriteBatch();
        for (Map.Entry<Cell, Value> e : values) {
            Cell cell = e.getKey();
            byte[] contents = e.getValue().getContents();
            long timestamp = e.getValue().getTimestamp();
            batch.put(getKey(tableName, cell, timestamp), contents);
        }
        try {
            db.write(batch, writeOptions);
        } catch (DBException e) {
            throw new RuntimeException("Failed on put: " + Iterables.transform(values,
                    new Function<Map.Entry<Cell, Value>, Cell>() {
                @Override
                public Cell apply(Entry<Cell, Value> entry) {
                    return entry.getKey();
                }
            }), e);
        }
    }

    @Override
    public void putUnlessExists(String tableName, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        Validate.isTrue(isTransactionTable(tableName));
        putTransaction(values);
    }


    private void putTransaction(Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        final LockState<Cell> lockToken = lockSet.lockOnObjects(values.keySet());
        try {
            final Set<Cell> alreadyExists = Sets.newHashSet();
            for (Cell cell : values.keySet()) {
                Validate.isTrue(Arrays.equals(
                        cell.getColumnName(),
                        COMMIT_TS_COLUMN));
                final byte[] key = getKey(TRANSACTION_TABLE, cell, TRANSACTION_TS);
                if (keyExists(TRANSACTION_TABLE, key)) {
                    alreadyExists.add(cell);
                }
            }
            final WriteOptions writeOptions = new WriteOptions();
            writeOptions.sync(true);
            Collection<Entry<Cell, Value>> filteredValues = KeyValueServices.toConstantTimestampValues(
                    Maps.filterKeys(values, Predicates.not(Predicates.in(alreadyExists))).entrySet(),
                    TRANSACTION_TS);
            putInternal(TRANSACTION_TABLE, filteredValues, writeOptions);
            if (!alreadyExists.isEmpty()) {
                throw new KeyAlreadyExistsException("keys already exist", alreadyExists);
            }
        } finally {
            lockToken.unlock();
        }
    }

    @Override
    public void delete(String tableName, Multimap<Cell, Long> keys) {
        Validate.isTrue(!isTransactionTable(tableName));
        final WriteOptions writeOptions = new WriteOptions();
        final WriteBatch batch = createWriteBatch();
        for (Map.Entry<Cell, Long> e : keys.entries()) {
            batch.delete(getKey(tableName, e.getKey(), e.getValue()));
        }
        try {
            db.write(batch, writeOptions);
        } catch (DBException e) {
            throw new RuntimeException(tableName + keys.keySet(), e);
        }
    }


    @Override
    public ClosableIterator<RowResult<Value>> getRange(String tableName,
                                                       RangeRequest rangeRequest,
                                                       long timestamp) {
        byte[] tablePrefix = getKeyPrefixWithTable(tableName);
        DBIterator iter = getDBIterator(tableName, tablePrefix, rangeRequest);
        return new ValueRangeIterator(iter, tablePrefix, rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        byte[] tablePrefix = getKeyPrefixWithTable(tableName);
        DBIterator iter = getDBIterator(tableName, tablePrefix, rangeRequest);
        return new TimestampRangeIterator(iter, tablePrefix, rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        byte[] tablePrefix = getKeyPrefixWithTable(tableName);
        DBIterator iter = getDBIterator(tableName, tablePrefix, rangeRequest);
        return new HistoryRangeIterator(iter, tablePrefix, rangeRequest, timestamp);
    }

    private DBIterator getDBIterator(String tableName,
                                     byte[] tablePrefix,
                                     RangeRequest rangeRequest) {
        if (rangeRequest.isReverse()) {
            throw new UnsupportedOperationException();
        }
        final DBIterator it = db.iterator(getReadOptionsForTable(tableName));
        try {
            if (rangeRequest.getStartInclusive().length != 0) {
                it.seek(LevelDbKeyValueUtils.getKey(tablePrefix, Cells.createSmallestCellForRow(rangeRequest.getStartInclusive()), 0L));
            } else {
                it.seek(tablePrefix);
            }
            return it;
        } catch (DBException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public synchronized void truncateTable(String tableName){
        int maxValueSize = Integer.MAX_VALUE; //Safe (but not great) fallback in case this doesn't work
        byte[] rawMetadata = getMetadataForTable(tableName);
        if (rawMetadata.length != 0){
            TableMetadata tableMetadata = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(rawMetadata);
            ColumnMetadataDescription columns = tableMetadata.getColumns();
            maxValueSize = columns.getMaxValueSize();
        }

        dropTable(tableName);
        createTable(tableName, maxValueSize);
    }

    @Override
    public synchronized void dropTable(String tableName) {
        final Cell meta = Cell.create(tableName.getBytes(), METADATA_COL);
        final Cell size = Cell.create(tableName.getBytes(), SIZE_COL);
        final Cell encoded = Cell.create(tableName.getBytes(), ENCODED_COL);
        try {
            final WriteOptions writeOptions = new WriteOptions();
            writeOptions.sync(true);
            db.delete(getKey(META_TABLE, meta, TRANSACTION_TS), writeOptions);
            db.delete(getKey(META_TABLE, size, TRANSACTION_TS), writeOptions);
            db.delete(getKey(META_TABLE, encoded, TRANSACTION_TS), writeOptions);
            fastTables.remove(tableName);
            encodedName.remove(tableName);

            // TODO: actually remove all the data from those tables
        } catch (DBException e) {
            Throwables.throwUncheckedException(e);
        }
    }


    @Override
    public synchronized void createTable(String tableName, int maxValueSizeInBytes) {
        Validate.isTrue(maxValueSizeInBytes > 0);
        internalPutMetaForTable(tableName, new byte[0], maxValueSizeInBytes);
    }


    private void internalPutMetaForTable(String tableName, byte[] metaData, int maxValueSize) {
        final boolean isCreate = maxValueSize > 0;
        final Cell meta = Cell.create(tableName.getBytes(), METADATA_COL);
        final Cell size = Cell.create(tableName.getBytes(), SIZE_COL);
        final Cell encoded = Cell.create(tableName.getBytes(), ENCODED_COL);
        try {
            if (isCreate && keyExists(META_TABLE, getKey(META_TABLE, meta, TRANSACTION_TS))) {
                return;
            }
            final WriteOptions writeOptions = new WriteOptions();
            writeOptions.sync(true);
            final WriteBatch batch = createWriteBatch();
            batch.put(getKey(META_TABLE, meta, TRANSACTION_TS), metaData);
            if (isCreate) {
                batch.put(getKey(META_TABLE, size, TRANSACTION_TS), EncodingUtils.encodeVarLong(maxValueSize));
//                batch.put(getKey(META_TABLE, encoded, TRANSACTION_TS), adsf);
            }
            db.write(batch, writeOptions);
            if (isCreate && maxValueSize <= MAX_SMALL_TABLE_SIZE) {
                fastTables.add(tableName);
            }
        } catch (DBException e) {
            Throwables.throwUncheckedException(e);
        }
    }


    private boolean keyExists(String tableName, byte[] key) {
        final DBIterator iterator = db.iterator(getReadOptionsForTable(tableName));
        try {
            iterator.seek(key);
            return iterator.hasNext() && Arrays.equals(key, iterator.peekNext().getKey());
        } catch (DBException e) {
            throw Throwables.throwUncheckedException(e);
        } finally {
            try {
                iterator.close();
            } catch (IOException e) {
                log.warn("failed to close", e);
            }
        }
    }


    @Override
    public Set<String> getAllTableNames() {
        return Sets.newHashSet(getAllTablesAndSizes().keySet());
    }


    private Map<String, Integer> getAllTablesAndSizes() {
        final Map<String, Integer> ret = Maps.newHashMap();
        final RangeRequest rangeRequest = RangeRequest.builder().build();
        final ClosableIterator<RowResult<Value>> it = getRange(META_TABLE, rangeRequest, TRANSACTION_TS+1);
        try {
            while (it.hasNext()) {
                final RowResult<Value> row = it.next();
                final String tableName = new String(row.getRowName());
                final long size = EncodingUtils.decodeVarLong(row.getColumns().get(SIZE_COL).getContents());
                ret.put(tableName, (int)size);
            }
            return ret;
        } finally {
            it.close();
        }
    }


    @Override
    public byte[] getMetadataForTable(String tableName) {
        final Cell meta = Cell.create(tableName.getBytes(), METADATA_COL);
        try {
            return db.get(getKey(META_TABLE, meta, TRANSACTION_TS),
                          getReadOptionsForTable(META_TABLE));
        } catch (DBException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }


    @Override
    public synchronized void putMetadataForTable(String tableName, byte[] metadata) {
        internalPutMetaForTable(tableName, metadata, -1);
    }


    private ReadOptions getReadOptionsForTable(String tableName) {
        final ReadOptions readOptions = new ReadOptions();
        readOptions.fillCache(fastTables.contains(tableName));
        return readOptions;
    }


    private WriteBatch createWriteBatch() {
        return db.createWriteBatch();
    }


    @VisibleForTesting
    static DBFactory getDBFactory() {
        return new JniDBFactory();
    }


    private static byte[] getKey(String tableName, Cell cell, long timeStamp) {
        Validate.isTrue(EncodingUtils.sizeOfVarLong(cell.getRowName().length) <= 2);
        Validate.isTrue(EncodingUtils.sizeOfVarLong(cell.getColumnName().length) <= 2);
        final byte[] rowSize = EncodingUtils.encodeVarLong(cell.getRowName().length);
        final byte[] colSize = EncodingUtils.encodeVarLong(cell.getColumnName().length);
        ArrayUtils.reverse(colSize);
        ArrayUtils.reverse(rowSize);
        return com.google.common.primitives.Bytes.concat(
            getKeyPrefixWithTable(tableName),
            cell.getRowName(),
            cell.getColumnName(),
            EncodingUtils.encodeVarLong(timeStamp),
            colSize,
            rowSize);
    }


    private static byte[] getKeyPrefixWithTable(String tableName) {
        if (META_TABLE.equals(tableName)) {
            return new byte[] { ',' };
        } else {
            validateTableName(tableName);
            return ("--" + tableName + ',').getBytes();
        }
    }


    private static boolean isTransactionTable(String tableName) {
        return TRANSACTION_TABLE.equals(tableName);
    }

    @Override
    @NonIdempotent
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        for (Map.Entry<String, ? extends Map<Cell, byte[]>> e : valuesByTable.entrySet()) {
            put(e.getKey(), e.getValue(), timestamp);
        }
    }

    @Override
    @Idempotent
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName,
                                                                                                    Iterable<RangeRequest> rangeRequests,
                                                                                                    long timestamp) {
        return KeyValueServices.getFirstBatchForRangesUsingGetRange(this, tableName, rangeRequests, timestamp);
    }

    @Override
    @Idempotent
    public Multimap<Cell, Long> getAllTimestamps(String tableName, Set<Cell> cells, long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Idempotent
    public void addGarbageCollectionSentinelValues(String tableName, Set<Cell> cells) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void initializeFromFreshInstance() {
        // nothing needed
    }

    @Override
    public void teardown() {
        //
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableList.of();
    }


    @Override
    public void truncateTables(Set<String> tableNames) throws InsufficientConsistencyException {
        for (String table : tableNames) {
            truncateTable(table);
        }
    }


    @Override
    public void createTables(Map<String, Integer> tableNamesToMaxValueSizeInBytes)
            throws InsufficientConsistencyException {
        for (Map.Entry<String, Integer> e : tableNamesToMaxValueSizeInBytes.entrySet()) {
            createTable(e.getKey(), e.getValue());
        }
    }


    @Override
    public Map<String, byte[]> getMetadataForTables() {
        Map<String, byte[]> ret = Maps.newHashMap();
        for (String table : getAllTableNames()) {
            ret.put(table, getMetadataForTable(table));
        }
        return ret;
    }


    @Override
    public void putMetadataForTables(Map<String, byte[]> tableNameToMetadata) {
        for (Map.Entry<String, byte[]> e : tableNameToMetadata.entrySet()) {
            putMetadataForTable(e.getKey(), e.getValue());
        }
    }


    @Override
    public void compactInternally(String tableName) {
    }
}
