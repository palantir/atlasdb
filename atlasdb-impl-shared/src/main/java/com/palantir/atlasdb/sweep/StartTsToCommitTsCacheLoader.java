/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.sweep;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheLoader;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;

public class StartTsToCommitTsCacheLoader extends CacheLoader<Long, Long> {
    private static final Logger log = LoggerFactory.getLogger(StartTsToCommitTsCacheLoader.class);

    private final TransactionService transactionService;

    public StartTsToCommitTsCacheLoader(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @Override
    public Long load(Long startTs) {
        Long commitTs = transactionService.get(startTs);

        if (commitTs != null) {
            return commitTs;
        }

        // Roll back this transaction (note that rolling back arbitrary transactions
        // can never cause correctness issues, only liveness issues)
        try {
            // TODO: carrino: use the batched version of putUnlessExists when it is available.
            transactionService.putUnlessExists(startTs, TransactionConstants.FAILED_COMMIT_TS);
        } catch (KeyAlreadyExistsException e) {
            String msg = "Could not roll back transaction with start timestamp " + startTs + "; either" +
                    " it was already rolled back (by a different transaction), or it committed successfully" +
                    " before we could roll it back.";
            log.error("This isn't a bug but it should be very infrequent. " + msg,
                    new TransactionFailedRetriableException(msg, e));
        }

        commitTs = transactionService.get(startTs);
        return Preconditions.checkNotNull(commitTs);
    }
}
