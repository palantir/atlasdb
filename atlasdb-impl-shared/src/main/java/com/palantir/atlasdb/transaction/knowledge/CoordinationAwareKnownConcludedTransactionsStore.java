/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.knowledge;

import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class CoordinationAwareKnownConcludedTransactionsStore {
    private final CoordinationService<InternalSchemaMetadata> coordinationService;
    private final KnownConcludedTransactionsStore delegate;

    public CoordinationAwareKnownConcludedTransactionsStore(
            CoordinationService coordinationService,
            KnownConcludedTransactionsStore delegate) {
        this.coordinationService = coordinationService;
        this.delegate = delegate;
    }

    public Optional<TimestampRangeSet> get() {
        return delegate.get();
    }

    public void supplement(Range<Long> timestampRangeToAdd) {
        //todo(snanda): this is not the right api
        Optional<ValueAndBound<InternalSchemaMetadata>> lastKnownLocalValue =
                coordinationService.getLastKnownLocalValue();

        Map<Range<Long>, Integer> longIntegerRangeMap = lastKnownLocalValue
                .orElseThrow(() -> new SafeIllegalStateException("Unexpectedly found no value in store"))
                .value()
                .orElseThrow(() -> new SafeIllegalStateException("Unexpectedly found no value in store"))
                .timestampToTransactionsTableSchemaVersion()
                .rangeMapView()
                .asMapOfRanges();

        Set<Range<Long>> rangesOnTTS = KeyedStream.stream(longIntegerRangeMap)
                .filter(schemaVersion -> schemaVersion.equals(TransactionConstants.TTS_TRANSACTIONS_SCHEMA_VERSION))
                .keys()
                .collect(Collectors.toSet());
        Preconditions.checkState(rangesOnTTS.size() == 1, "Should have exactly one range for TTS schema.");
        Range<Long> timestampRangeOnTxn4 = timestampRangeToAdd.intersection(Iterables.getOnlyElement(rangesOnTTS));
        delegate.supplement(timestampRangeOnTxn4);
    }
}
