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

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.stream.StreamTableType;
import com.palantir.atlasdb.transaction.api.Transaction;

public class StreamStoreRemappingNextTableToSweepProviderImpl implements NextTableToSweepProvider {
    private NextTableToSweepProviderImpl delegate;
    private boolean hasRemappedStreamStoreValueTable = false;
    private TableReference previousStreamStoreValueTable = null;

    public StreamStoreRemappingNextTableToSweepProviderImpl(NextTableToSweepProviderImpl delegate) {
        this.delegate = delegate;
    }

    @Override
    public Optional<TableReference> chooseNextTableToSweep(Transaction tx, long conservativeSweepTs) {
        if (hasRemappedStreamStoreValueTable) {
            Preconditions.checkState(previousStreamStoreValueTable != null,
                    "we should have set previousStreamStoreValueTable on the previous iteration");
            hasRemappedStreamStoreValueTable = false;
            return Optional.of(previousStreamStoreValueTable);
        }

        Optional<TableReference> tableReferenceOptional = delegate.chooseNextTableToSweep(tx, conservativeSweepTs);
        if (!tableReferenceOptional.isPresent()) {
            return tableReferenceOptional;
        }

        TableReference tableReference = tableReferenceOptional.get();
        if (!StreamTableType.isStreamStoreValueTable(tableReference)) {
            return Optional.of(tableReference);
        }

        previousStreamStoreValueTable = tableReference;
        hasRemappedStreamStoreValueTable = true;
        return Optional.of(StreamTableType.getIndexFromValue(tableReference));
    }
}
