/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.priority;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.stream.StreamTableType;
import com.palantir.atlasdb.transaction.api.Transaction;

public class StreamStoreRemappingNextTableToSweepProviderImpl implements NextTableToSweepProvider {
    private static final long MILLIS_TO_WAIT_BEFORE_SWEEP_VALUE_TABLE = TimeUnit.HOURS.toMillis(1);

    private NextTableToSweepProviderImpl delegate;
    private State state = State.DEFAULT;
    private TableReference streamStoreValueTable = null;
    private long instantIndexTableWasSwept = 0L;

    public StreamStoreRemappingNextTableToSweepProviderImpl(NextTableToSweepProviderImpl delegate) {
        this.delegate = delegate;
    }

    @Override
    public Optional<TableReference> chooseNextTableToSweep(Transaction tx, long conservativeSweepTs) {
        switch (state) {
            case DEFAULT:
                Optional<TableReference> tableReferenceOptional =
                        delegate.chooseNextTableToSweep(tx, conservativeSweepTs);
                if (!tableReferenceOptional.isPresent()) {
                    return tableReferenceOptional;
                }

                TableReference tableReference = tableReferenceOptional.get();
                if (!StreamTableType.isStreamStoreValueTable(tableReference)) {
                    return Optional.of(tableReference);
                }

                streamStoreValueTable = tableReference;
                state = State.SWEEPING_INDEX_TABLE;
                return Optional.of(StreamTableType.getIndexTableFromValueTable(tableReference));

            case SWEEPING_INDEX_TABLE:
                instantIndexTableWasSwept = System.currentTimeMillis();
                state = State.WAITING_TO_SWEEP_VALUE_TABLE;
                return delegate.chooseNextTableToSweep(tx, conservativeSweepTs);

            case WAITING_TO_SWEEP_VALUE_TABLE:
                if (System.currentTimeMillis() - instantIndexTableWasSwept < MILLIS_TO_WAIT_BEFORE_SWEEP_VALUE_TABLE) {
                    return delegate.chooseNextTableToSweep(tx, conservativeSweepTs);
                }
                state = State.DEFAULT;
                return Optional.of(streamStoreValueTable);
        }

        return Optional.empty();
    }

    private enum State {
        DEFAULT,
        SWEEPING_INDEX_TABLE,
        WAITING_TO_SWEEP_VALUE_TABLE,
    }
}
