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
package com.palantir.atlasdb.transaction.service;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosRoundFailureException;

/**
 * This transaction service is a proof of concept and experimental and should not be used in production.
 */
public class PaxosTransactionService implements TransactionService {
    private static final Logger log = LoggerFactory.getLogger(PaxosTransactionService.class);

    private final PaxosProposer proposer;
    private final TransactionKVSWrapper kvStore;

    public static TransactionService create(PaxosProposer proposer, TransactionKVSWrapper kvStore) {
        return new PaxosTransactionService(proposer, kvStore);
    }

    private PaxosTransactionService(PaxosProposer proposer, TransactionKVSWrapper kvStore) {
        this.proposer = proposer;
        this.kvStore = kvStore;
    }

    @Override
    public Long get(long startTimestamp) {
        return kvStore.get(startTimestamp);
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        return kvStore.get(startTimestamps);
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) throws KeyAlreadyExistsException {
        try {
            long finalCommitTs = getFinalCommitTs(startTimestamp, commitTimestamp);
            try {
                // Make sure we do this put before we return because we want #get to succeed.
                kvStore.putAll(ImmutableMap.of(startTimestamp, finalCommitTs));
            } catch (KeyAlreadyExistsException e) {
                // This case is worrisome because KV is required to not throw on an idempotent put.
                log.error("KV threw on put.  This is likely a bug in the KV store because it shouldn't throw in this case", e);
            }
            if (commitTimestamp != finalCommitTs) {
                throw new KeyAlreadyExistsException("Key " + startTimestamp + " already exists and is mapped to " + finalCommitTs);
            }
        } catch (PaxosRoundFailureException e) {
            throw new ServiceNotAvailableException("Could not store trascaction");
        }
    }

    private long getFinalCommitTs(long startTimestamp, long commitTimestamp) throws PaxosRoundFailureException {
        try {
            proposer.propose(startTimestamp, PtBytes.toBytes(commitTimestamp)).getData();
        } catch (PaxosRoundFailureException e) {
            // Allow first proposal to fail to ensure our proposal id is caught up.
        }
        // If the previous propose was successful, this will be fast because we will have already
        // learned the value.
        byte[] finalValue = proposer.propose(startTimestamp, PtBytes.toBytes(commitTimestamp)).getData();
        long finalCommitTs = PtBytes.toLong(finalValue);
        return finalCommitTs;
    }

}
