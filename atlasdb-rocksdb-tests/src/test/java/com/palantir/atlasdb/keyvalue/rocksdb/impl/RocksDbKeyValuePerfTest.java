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
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.util.Pair;

@Ignore
public final class RocksDbKeyValuePerfTest {
    private static final Random RAND = new Random();
    private static final int KEY_SIZE = 16;
    private static final int VALUE_SIZE = 100;
    private static final int BATCH_SIZE = 1000;
    private final ExecutorService executor = PTExecutors.newCachedThreadPool();

    private RocksDbKeyValueService db = null;

    @Before
    public void setUp() throws IOException {
        File dbPath = getDatabasePath();
        db = RocksDbKeyValueService.create(dbPath.getAbsolutePath());
        for (String table : db.getAllTableNames()) {
            db.dropTable(table);
        }
        db.createTable("t", AtlasDbConstants.EMPTY_TABLE_METADATA);
    }

    @After
    public void tearDown() throws IOException {
        if (db != null) {
            db.close();
        }
    }

    @Test
    public void testWritePerf() throws ExecutionException, InterruptedException {
        final long startTime = System.currentTimeMillis();
        final Future<Pair<Long, Set<byte[]>>>
            f1 = submitWriteJob(0, BATCH_SIZE / 4),
            f2 = submitWriteJob(BATCH_SIZE / 4, BATCH_SIZE / 2),
            f3 = submitWriteJob(BATCH_SIZE / 2, 3 * BATCH_SIZE / 4),
            f4 = submitWriteJob(3 * BATCH_SIZE / 4, BATCH_SIZE);
        final long rawBytes = f1.get().lhSide
                              + f2.get().lhSide
                              + f3.get().lhSide
                              + f4.get().lhSide;
        final long elapsedTime = System.currentTimeMillis() - startTime;
        final double elapsedSeconds = elapsedTime / 1000.0;
        final double megs = rawBytes / (1024.0 * 1024.0);
        System.out.println("MB = " + megs);
        System.out.println("MB/s = " + (megs/elapsedSeconds));
    }

    private Pair<Long, Set<byte[]>> doSomeWrites(int batchStart, int batchEnd) {
        assert batchStart >= 0;
        assert batchEnd >= 0;
        assert batchStart <= batchEnd;
        final Set<byte[]> ret = Sets.newTreeSet(UnsignedBytes.lexicographicalComparator());
        long rawBytes = 0;
        for (int i = batchStart; i != batchEnd; ++i) {
            final Map<Cell, byte[]> toPut = Maps.newHashMap();
            for (int j = 0; j != BATCH_SIZE; ++j) {
                toPut.put(Cell.create(getRandomBytes(KEY_SIZE), PtBytes.toBytes("t")), getRandomBytes(VALUE_SIZE));
                rawBytes += KEY_SIZE + VALUE_SIZE + 1 + 4;
            }
            db.put("t", toPut, i);
//            Cell commitCell = Cell.create(EncodingUtils.encodeVarLong(i), TransactionConstants.COMMIT_TS_COLUMN);
//            ImmutableMap<Cell, byte[]> commitMap = ImmutableMap.of(commitCell, EncodingUtils.encodeVarLong(i+1));
//            db.put(TransactionConstants.TRANSACTION_TABLE, commitMap, 0);
        }
        return Pair.create(rawBytes, ret);
    }

    private Future<Pair<Long, Set<byte[]>>> submitWriteJob(final int batchStart, final int batchEnd) {
        return executor.submit(new Callable<Pair<Long, Set<byte[]>>>() {
            @Override
            public Pair<Long, Set<byte[]>> call() throws Exception {
                return doSomeWrites(batchStart, batchEnd);
            }
        });
    }

    private static byte[] getRandomBytes(int numBytes) {
        final byte[] ret = new byte[numBytes];
        RAND.nextBytes(ret);
        return ret;
    }

    private static File getDatabasePath() {
        return new File("./testdb");
    }
}
