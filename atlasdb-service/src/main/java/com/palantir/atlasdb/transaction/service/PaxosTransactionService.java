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

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosRoundFailureException;

public class PaxosTransactionService extends AbstractTransactionService {
    private final PaxosProposer proposer;

    public static TransactionService create(PaxosProposer proposer, KeyValueService kvs) {
        return new PaxosTransactionService(proposer, kvs);
    }

    private PaxosTransactionService(PaxosProposer proposer, KeyValueService kvs) {
        super(kvs);
        this.proposer = proposer;
    }

    @Override
    public Long get(long startTimestamp) {
        Cell cell = getTransactionCell(startTimestamp);
        Map<Cell, Value> returnMap = kvs.get(TransactionConstants.TRANSACTION_TABLE,
                ImmutableMap.of(cell, MAX_TIMESTAMP));
        if (returnMap.containsKey(cell)) {
            return TransactionConstants.getTimestampForValue(returnMap.get(cell).getContents());
        } else {
            return null;
        }
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        Map<Long, Long> result = Maps.newHashMap();
        Map<Cell, Long> startTsMap = Maps.newHashMap();
        for (Long startTimestamp : startTimestamps) {
            Cell cell = getTransactionCell(startTimestamp);
            startTsMap.put(cell, MAX_TIMESTAMP);
        }

        Map<Cell, Value> rawResults = kvs.get(TransactionConstants.TRANSACTION_TABLE, startTsMap);
        for (Map.Entry<Cell, Value> e : rawResults.entrySet()) {
            long startTs = TransactionConstants.getTimestampForValue(e.getKey().getRowName());
            long commitTs = TransactionConstants.getTimestampForValue(e.getValue().getContents());
            result.put(startTs, commitTs);
        }

        return result;
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) throws KeyAlreadyExistsException {
        try {
            byte[] finalValue = proposer.propose(startTimestamp, PtBytes.toBytes(commitTimestamp));
            long finalCommitTs = PtBytes.toLong(finalValue);
            try {
                // Make sure we do this put before we return because we want #get to succeed.
                putAll(ImmutableMap.of(startTimestamp, finalCommitTs));
            } catch (KeyAlreadyExistsException e) {
                // this case isn't worrisome
            }
            if (commitTimestamp != finalCommitTs) {
                throw new KeyAlreadyExistsException("Key " + startTimestamp + " already exists and is mapped to " + finalCommitTs);
            }
        } catch (PaxosRoundFailureException e) {
            throw new ServiceNotAvailableException("Could not store transaction");
        }
    }

    private void putAll(Map<Long, Long> timestampMap) throws KeyAlreadyExistsException {
        Map<Cell, byte[]> kvMap = new HashMap<>();
        for (Map.Entry<Long, Long> entry : timestampMap.entrySet()) {
            kvMap.put(
                    getTransactionCell(entry.getKey()),
                    TransactionConstants.getValueForTimestamp(entry.getValue()));
        }

        kvs.put(TransactionConstants.TRANSACTION_TABLE, kvMap, 0); // This can throw unchecked exceptions
    }

}
