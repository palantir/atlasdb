/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.transaction.api;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.lockwatch.LockWatchRequest;
import com.palantir.atlasdb.lockwatch.LockWatchState;
import com.palantir.atlasdb.lockwatch.NamespacedLockWatchingRpcClient;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;

public class SimpleTransactionRowLockWatchingService implements TransactionLockWatchingService {
    private final UUID serviceId = UUID.randomUUID();
    private final NamespacedLockWatchingRpcClient rpcClient;

    public SimpleTransactionRowLockWatchingService(NamespacedLockWatchingRpcClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    @Override
    public LockDescriptorMapping registerRowWatches(TableReference tableRef, Set<byte[]> rowNames) {
        LockDescriptorMapping result = LockDescriptorMapping.of(KeyedStream.of(rowNames)
                .mapKeys(row -> toLockDescriptor(tableRef, row))
                .map(row -> RowReference.of(tableRef, row))
                .collectToMap());
        rpcClient.startWatching(LockWatchRequest.of(serviceId, result.mapping().keySet()));
        return result;
    }

    @Override
    public void deregisterWatches(TableReference tableRef, Set<byte[]> rowNames) {
        Set<LockDescriptor> descriptors = rowNames.stream()
                .map(row -> toLockDescriptor(tableRef, row))
                .collect(Collectors.toSet());
        rpcClient.stopWatching(LockWatchRequest.of(serviceId, descriptors));
    }

    @Override
    public LockWatchState getLockWatchState() {
        return rpcClient.getWatchState(serviceId);
    }

    private LockDescriptor toLockDescriptor(TableReference tableRef, byte[] rowName) {
        return AtlasRowLockDescriptor.of(tableRef.getQualifiedName(), rowName);
    }
}
