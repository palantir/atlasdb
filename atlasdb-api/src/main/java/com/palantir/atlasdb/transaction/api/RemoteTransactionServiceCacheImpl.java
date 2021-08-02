/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.palantir.atlasdb.transaction.service.TransactionService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class RemoteTransactionServiceCacheImpl implements RemoteTransactionServiceCache {
    private final LoadingCache<String, TransactionService> remoteTransactionServices;

    public RemoteTransactionServiceCacheImpl(
            LoadingCache<String, TransactionService> remoteTransactionServices) {
        this.remoteTransactionServices = remoteTransactionServices;
    }

    public static RemoteTransactionServiceCache create(Function<String, TransactionService> transactionServiceFactory) {
        return new RemoteTransactionServiceCacheImpl(Caffeine.newBuilder()
                .expireAfterAccess(5, TimeUnit.DAYS)
                .build(transactionServiceFactory::apply));
    }

    @Override
    public TransactionService getOrCreateForNamespace(String namespace) {
        return remoteTransactionServices.get(namespace);
    }
}
