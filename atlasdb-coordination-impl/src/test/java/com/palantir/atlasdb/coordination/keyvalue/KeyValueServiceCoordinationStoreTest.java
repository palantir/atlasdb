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

package com.palantir.atlasdb.coordination.keyvalue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.coordination.CoordinationStore;
import com.palantir.atlasdb.coordination.ImmutableSequenceAndBound;
import com.palantir.atlasdb.coordination.SequenceAndBound;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.impl.ImmutableCheckAndSetResult;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;

import okio.ByteString;

public class KeyValueServiceCoordinationStoreTest {
    private static final byte[] COORDINATION_KEY = PtBytes.toBytes("aaaaa");

    private static final long SEQUENCE_NUMBER_1 = 1L;
    private static final long SEQUENCE_NUMBER_2 = 111L;
    private static final ByteString VALUE_1 = ByteString.of(PtBytes.toBytes("blabla"));
    private static final ByteString VALUE_2 = ByteString.of(PtBytes.toBytes("testtest"));
    private static final SequenceAndBound SEQUENCE_AND_BOUND_1 = ImmutableSequenceAndBound.of(1, 2);
    private static final SequenceAndBound SEQUENCE_AND_BOUND_2 = ImmutableSequenceAndBound.of(3, 4);

    private final InMemoryKeyValueService kvs = new InMemoryKeyValueService(true);
    private final CoordinationStore coordinationStore
            = KeyValueServiceCoordinationStore.create(kvs, COORDINATION_KEY);

    @Test
    public void getReturnsEmptyIfNoKeyFound() {
        assertThat(coordinationStore.getValue(SEQUENCE_NUMBER_1)).isEmpty();
    }

    @Test
    public void retrievesCorrectStoredKeys() {
        coordinationStore.putValue(SEQUENCE_NUMBER_1, VALUE_1);
        coordinationStore.putValue(SEQUENCE_NUMBER_2, VALUE_2);
        assertThat(coordinationStore.getValue(SEQUENCE_NUMBER_1))
                .contains(VALUE_1);
        assertThat(coordinationStore.getValue(SEQUENCE_NUMBER_2))
                .contains(VALUE_2);
    }

    @Test
    public void throwsIfAttemptingToGetAtNegativeSequenceNumber() {
        assertThatThrownBy(() -> coordinationStore.getValue(-1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Only positive sequence numbers are supported");
    }

    @Test
    public void throwsIfAttemptingToPutAtNegativeSequenceNumber() {
        assertThatThrownBy(() -> coordinationStore.putValue(-1, VALUE_1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Only positive sequence numbers are supported");
    }

    @Test
    public void getCoordinationValueReturnsEmptyIfNotSet() {
        assertThat(coordinationStore.getCoordinationValue()).isEmpty();
    }

    @Test
    public void storesAndRetrievesCoordinationValues() {
        assertThat(coordinationStore.checkAndSetCoordinationValue(Optional.empty(), SEQUENCE_AND_BOUND_1))
                .isEqualTo(ImmutableCheckAndSetResult.of(true, ImmutableList.of(SEQUENCE_AND_BOUND_1)));
        assertThat(coordinationStore.getCoordinationValue())
                .contains(SEQUENCE_AND_BOUND_1);
    }

    @Test
    public void canCheckAndSetBetweenValues() {
        coordinationStore.checkAndSetCoordinationValue(Optional.empty(), SEQUENCE_AND_BOUND_1);
        assertThat(coordinationStore.checkAndSetCoordinationValue(
                Optional.of(SEQUENCE_AND_BOUND_1), SEQUENCE_AND_BOUND_2))
                .isEqualTo(ImmutableCheckAndSetResult.of(true, ImmutableList.of(SEQUENCE_AND_BOUND_2)));
    }

    @Test
    public void checkAndSetFailsIfOldValueNotCorrect() {
        coordinationStore.checkAndSetCoordinationValue(Optional.empty(), SEQUENCE_AND_BOUND_1);
        assertThat(coordinationStore.checkAndSetCoordinationValue(
                Optional.empty(), SEQUENCE_AND_BOUND_2))
                .isEqualTo(ImmutableCheckAndSetResult.of(false, ImmutableList.of(SEQUENCE_AND_BOUND_1)));
    }

    @Test
    public void multipleStoresCanCoexist() {
        byte[] alternateCoordinationKey = PtBytes.toBytes("bbbbb");
        CoordinationStore alternateCoordinationStore
                = KeyValueServiceCoordinationStore.create(kvs, alternateCoordinationKey);
        alternateCoordinationStore.putValue(SEQUENCE_NUMBER_1, VALUE_1);
        assertThat(coordinationStore.getValue(SEQUENCE_NUMBER_1)).isEmpty();
    }
}
