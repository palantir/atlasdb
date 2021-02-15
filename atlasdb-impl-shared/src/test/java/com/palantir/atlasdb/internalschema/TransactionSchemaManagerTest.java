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

package com.palantir.atlasdb.internalschema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Optional;
import org.junit.Test;

@SuppressWarnings("unchecked") // Mocks of generic types
public class TransactionSchemaManagerTest {
    private static final long TIMESTAMP_1 = 12345;
    private static final long TIMESTAMP_2 = 34567L;

    private final TransactionSchemaManager manager = new TransactionSchemaManager(mock(CoordinationService.class));

    @Test
    public void getsRangeAtBoundThresholdThrowsIfNoValueActuallyPresent() {
        assertThatThrownBy(() -> manager.getRangeAtBoundThreshold(ValueAndBound.of(Optional.empty(), TIMESTAMP_1)))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("Unexpectedly found no value in store");
    }

    @Test
    public void getRangeAtBoundThresholdWorksOnSingleRanges() {
        assertThat(manager.getRangeAtBoundThreshold(
                                ValueAndBound.of(InternalSchemaMetadata.defaultValue(), TIMESTAMP_1))
                        .getValue())
                .isEqualTo(TransactionConstants.DIRECT_ENCODING_TRANSACTIONS_SCHEMA_VERSION);
    }

    @Test
    public void getRangeAtBoundThresholdIdentifiesTheCorrectRange() {
        InternalSchemaMetadata internalSchemaMetadata = InternalSchemaMetadata.builder()
                .timestampToTransactionsTableSchemaVersion(
                        TimestampPartitioningMap.of(ImmutableRangeMap.<Long, Integer>builder()
                                .put(Range.closedOpen(1L, TIMESTAMP_1), 1)
                                .put(Range.closedOpen(TIMESTAMP_1, TIMESTAMP_2), 2)
                                .put(Range.atLeast(TIMESTAMP_2), 3)
                                .build()))
                .build();

        assertThat(manager.getRangeAtBoundThreshold(ValueAndBound.of(internalSchemaMetadata, 1L)))
                .satisfies(entry -> {
                    assertThat(entry.getKey()).isEqualTo(Range.closedOpen(1L, TIMESTAMP_1));
                    assertThat(entry.getValue()).isEqualTo(1);
                });
        assertThat(manager.getRangeAtBoundThreshold(ValueAndBound.of(internalSchemaMetadata, TIMESTAMP_1)))
                .satisfies(entry -> {
                    assertThat(entry.getKey()).isEqualTo(Range.closedOpen(TIMESTAMP_1, TIMESTAMP_2));
                    assertThat(entry.getValue()).isEqualTo(2);
                });
        assertThat(manager.getRangeAtBoundThreshold(ValueAndBound.of(internalSchemaMetadata, TIMESTAMP_2)))
                .satisfies(entry -> {
                    assertThat(entry.getKey()).isEqualTo(Range.atLeast(TIMESTAMP_2));
                    assertThat(entry.getValue()).isEqualTo(3);
                });
    }
}
