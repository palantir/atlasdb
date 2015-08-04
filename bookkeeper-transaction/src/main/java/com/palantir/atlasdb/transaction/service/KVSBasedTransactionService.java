/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.transaction.service;

import java.util.Map;

import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public class KVSBasedTransactionService implements TransactionService {
    private final TransactionKVSWrapper kvsWrapper;

    public KVSBasedTransactionService(KeyValueService keyValueService) {
        this.kvsWrapper = new TransactionKVSWrapper(keyValueService);
    }

    @Override
    public Long get(long startTimestamp) {
        return kvsWrapper.get(startTimestamp);
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        return kvsWrapper.get(startTimestamps);
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) throws KeyAlreadyExistsException {
        kvsWrapper.putUnlessExists(startTimestamp, commitTimestamp);
    }
}
