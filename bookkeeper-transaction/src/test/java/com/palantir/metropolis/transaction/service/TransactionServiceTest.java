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
package com.palantir.metropolis.transaction.service;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.service.InMemoryMetadataStorageService;
import com.palantir.atlasdb.transaction.service.InMemoryWriteAheadLog;
import com.palantir.atlasdb.transaction.service.MetadataStorageService;
import com.palantir.atlasdb.transaction.service.TransactionKVSWrapper;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServiceImpl;
import com.palantir.atlasdb.transaction.service.WriteAheadLogManager;

public class TransactionServiceTest {

    private static final ExecutorService executor = Executors.newCachedThreadPool();
    private static final long FLUSH_PERIOD = 100;

    @Test
    public void testRecovery() {
        WriteAheadLogManager manager = new InMemoryWriteAheadLog.InMemoryWriteAheadLogManager();
        MetadataStorageService metadataStorageService = new InMemoryMetadataStorageService();
        TransactionKVSWrapper kvsWrapper = new TransactionKVSWrapper(new InMemoryKeyValueService(true));
        TransactionService service = TransactionServiceImpl.create(manager, kvsWrapper, metadataStorageService, FLUSH_PERIOD);
        service.putUnlessExists(0, 1);
        service.putUnlessExists(1, 2);
        service = TransactionServiceImpl.create(manager, kvsWrapper, metadataStorageService, FLUSH_PERIOD);
        assert(service.get(0) == 1);
        assert(service.get(1) == 2);
        assert(service.get(2) == null);
    }

    @Test
    public void testConcurrentPutsHasOnlyOneSuccessful()
            throws InterruptedException, ExecutionException {
        final int ITERATORS_NUMBER = 100;

        WriteAheadLogManager manager = new InMemoryWriteAheadLog.InMemoryWriteAheadLogManager();
        MetadataStorageService metadataStorageService = new InMemoryMetadataStorageService();
        TransactionKVSWrapper kvsWrapper = new TransactionKVSWrapper(new InMemoryKeyValueService(true));
        final TransactionService service = TransactionServiceImpl.create(manager, kvsWrapper, metadataStorageService, FLUSH_PERIOD);
        final CountDownLatch latch = new CountDownLatch(ITERATORS_NUMBER);

        List<Future<Boolean>> futureList = Lists.newArrayList();
        for (int i = 0; i < ITERATORS_NUMBER; i++) {
            final int nr = i;
            Future<Boolean> task = executor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    try {
                        latch.countDown();
                        latch.await();
                        service.putUnlessExists(0, nr);
                        return true;
                    } catch (KeyAlreadyExistsException e) {
                        return false;
                    }
                }
            });
            futureList.add(task);
        }

        boolean keyAlreadyInMap = false;
        for (Future<Boolean> task: futureList) {
            boolean putWasSuccessful = task.get();
            assert(!(keyAlreadyInMap && putWasSuccessful));
            if (putWasSuccessful)
                keyAlreadyInMap = true;
        }

        assert (keyAlreadyInMap);
        assert (service.get(0L) != null);
    }

    @Test
    public void testConcurrentRandomPutsAndGets() throws InterruptedException, ExecutionException {
        final int ITERATORS_NUMBER = 10;
        final int CALLS_NUMBER = 10;

        WriteAheadLogManager manager = new InMemoryWriteAheadLog.InMemoryWriteAheadLogManager();
        MetadataStorageService metadataStorageService = new InMemoryMetadataStorageService();
        TransactionKVSWrapper kvsWrapper = new TransactionKVSWrapper(new InMemoryKeyValueService(true));
        final TransactionService service = TransactionServiceImpl.create(manager, kvsWrapper, metadataStorageService, FLUSH_PERIOD);

        Random r = new Random();
        final CountDownLatch latch = new CountDownLatch(ITERATORS_NUMBER*CALLS_NUMBER);

        List<Future<Boolean>> futureList = Lists.newArrayList();
        for (int i = 0; i < ITERATORS_NUMBER; i++) {
            final int index = i;
            for (int k = 0; k < CALLS_NUMBER; k++) {
                final long seed = r.nextLong();
                Future<Boolean> task = executor.submit(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        latch.countDown();
                        latch.await();
                        return randomTask(service, index, seed);
                    }
                });
                futureList.add(task);
            }
        }

        for (Future<Boolean> task: futureList) {
            assert(task.get());
        }
    }

    private static boolean randomTask(TransactionService service, long index, long randomSeed) {
        Random r = new Random(randomSeed);
        if (r.nextBoolean()) {
            Long value1 = service.get(index);
            Long value2 = service.get(index);
            return (value1 == null || value1.equals(value2));
        } else {
            Long value = r.nextLong();
            try {
                service.putUnlessExists(index, value);
                Long actualValue = service.get(index);
                return (actualValue != null && value.equals(actualValue));
            } catch (KeyAlreadyExistsException e) {
                return (service.get(index) != null);
            }
        }

    }
}
