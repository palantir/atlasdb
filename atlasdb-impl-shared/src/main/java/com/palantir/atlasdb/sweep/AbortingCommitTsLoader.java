/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.logsafe.SafeArg;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbortingCommitTsLoader implements CacheLoader<Long, Long> {
    private static final Logger log = LoggerFactory.getLogger(AbortingCommitTsLoader.class);
    private final TransactionService transactionService;

    public AbortingCommitTsLoader(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @Override
    public Long load(Long startTs) {
        Optional<Long> maybeCommitTs = tryGetFromTransactionService(startTs);

        if (!maybeCommitTs.isPresent()) {
            maybeCommitTs = tryToAbort(startTs);
        }

        if (!maybeCommitTs.isPresent()) {
            maybeCommitTs = tryGetFromTransactionService(startTs);
        }

        return maybeCommitTs.orElse(TransactionConstants.FAILED_COMMIT_TS);
    }

    @Override
    public Map<Long, Long> loadAll(Iterable<? extends Long> nonCachedKeys) {
        List<Long> missingKeys = ImmutableList.copyOf(nonCachedKeys);
        Map<Long, Long> result = new HashMap<>();

        Streams.stream(Lists.partition(missingKeys, AtlasDbConstants.TRANSACTION_TIMESTAMP_LOAD_BATCH_LIMIT))
                .forEach(batch -> result.putAll(transactionService.get(batch)));

        // roll back any uncommitted transactions
        missingKeys.stream()
                .filter(startTs -> !result.containsKey(startTs))
                .forEach(startTs -> result.put(startTs, load(startTs)));

        return result;
    }

    private Optional<Long> tryGetFromTransactionService(Long startTs) {
        return Optional.ofNullable(transactionService.get(startTs));
    }

    private Optional<Long> tryToAbort(Long startTs) {
        try {
            transactionService.putUnlessExists(startTs, TransactionConstants.FAILED_COMMIT_TS);
            return Optional.of(TransactionConstants.FAILED_COMMIT_TS);
        } catch (KeyAlreadyExistsException e) {
            log.info("Could not roll back transaction with start timestamp {}. Either it was already rolled back, or "
                    + "it committed successfully before we could roll it back. This isn't a bug but it should be "
                    + "very infrequent.", SafeArg.of("startTs", startTs));
            return Optional.empty();
        }
    }
}
