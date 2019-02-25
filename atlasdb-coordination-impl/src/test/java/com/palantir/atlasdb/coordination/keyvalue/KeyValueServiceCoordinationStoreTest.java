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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.coordination.CoordinationStore;
import com.palantir.atlasdb.coordination.ImmutableSequenceAndBound;
import com.palantir.atlasdb.coordination.SequenceAndBound;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
import com.palantir.atlasdb.keyvalue.impl.ImmutableCheckAndSetResult;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.conjure.java.serialization.ObjectMappers;

public class KeyValueServiceCoordinationStoreTest {
    private static final byte[] COORDINATION_ROW = PtBytes.toBytes("aaaaa");

    private static final long SEQUENCE_NUMBER_1 = 1L;
    private static final String VALUE_1 = "oneunoeinyi1";
    private static final String VALUE_2 = "twodoszweier2";
    private static final SequenceAndBound SEQUENCE_AND_BOUND_1 = ImmutableSequenceAndBound.of(1, 2);
    private static final SequenceAndBound SEQUENCE_AND_BOUND_2 = ImmutableSequenceAndBound.of(3, 4);
    private static final Function<ValueAndBound<String>, String> VALUE_PRESERVING_FUNCTION
            = valueAndBound -> valueAndBound.value()
                    .orElseThrow(() -> new IllegalStateException("Can only preserve a present value"));

    private final KeyValueService keyValueService = new InMemoryKeyValueService(true);
    private final AtomicLong timestampSequence = new AtomicLong();

    // Casting is reasonable because we initialize with false. We need the precise typing for some of the
    // tests that hit the store directly.
    private final KeyValueServiceCoordinationStore<String> coordinationStore
            = (KeyValueServiceCoordinationStore<String>) KeyValueServiceCoordinationStore.create(
                    ObjectMappers.newServerObjectMapper(),
                    keyValueService,
                    COORDINATION_ROW,
                    timestampSequence::incrementAndGet,
                    String.class,
                    false);

    @Test
    public void getReturnsEmptyIfNoKeyFound() {
        assertThat(coordinationStore.getAgreedValue()).isEmpty();
    }

    @Test
    public void canStoreAndRetrieveValues() {
        CheckAndSetResult<ValueAndBound<String>> casResult = coordinationStore.transformAgreedValue(unused -> VALUE_1);
        assertThat(casResult.successful()).isTrue();
        assertThat(Iterables.getOnlyElement(casResult.existingValues()).value()).contains(VALUE_1);
        assertThat(coordinationStore.getAgreedValue()).hasValueSatisfying(
                valueAndBound -> {
                    assertThat(valueAndBound.value()).contains(VALUE_1);
                    assertThat(valueAndBound.bound()).isGreaterThanOrEqualTo(0);
                });
    }

    @Test
    public void canApplyMultipleTransformations() {
        coordinationStore.transformAgreedValue(unused -> VALUE_1);
        ValueAndBound<String> firstValueAndBound = coordinationStore.getAgreedValue().get();
        coordinationStore.transformAgreedValue(unused -> VALUE_2);
        ValueAndBound<String> secondValueAndBound = coordinationStore.getAgreedValue().get();

        assertThat(firstValueAndBound.value()).contains(VALUE_1);
        assertThat(secondValueAndBound.value()).contains(VALUE_2);
        assertThat(firstValueAndBound.bound()).isLessThan(secondValueAndBound.bound());
    }

    @Test
    public void valuePreservingTransformationsAdvanceTheBound() {
        coordinationStore.transformAgreedValue(unused -> VALUE_1);
        ValueAndBound<String> firstValueAndBound = coordinationStore.getAgreedValue().get();
        coordinationStore.transformAgreedValue(VALUE_PRESERVING_FUNCTION);
        ValueAndBound<String> secondValueAndBound = coordinationStore.getAgreedValue().get();

        assertThat(firstValueAndBound.value()).contains(VALUE_1);
        assertThat(secondValueAndBound.value()).contains(VALUE_1);
        assertThat(firstValueAndBound.bound()).isLessThan(secondValueAndBound.bound());
    }

    @Test
    public void valuePreservingTransformationsDoNotWriteTheSameValueAgain() {
        coordinationStore.transformAgreedValue(unused -> VALUE_1);
        SequenceAndBound firstSequenceAndBound = coordinationStore.getCoordinationValue().get();
        coordinationStore.transformAgreedValue(VALUE_PRESERVING_FUNCTION);
        SequenceAndBound secondSequenceAndBound = coordinationStore.getCoordinationValue().get();

        assertThat(firstSequenceAndBound.sequence()).isEqualTo(secondSequenceAndBound.sequence());
        assertThat(firstSequenceAndBound.bound()).isLessThan(secondSequenceAndBound.bound());
    }

    @Test
    public void throwsIfAttemptingToGetAtNegativeSequenceNumber() {
        assertThatThrownBy(() -> coordinationStore.getValue(-1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Only positive sequence numbers are supported");
    }

    @Test
    public void throwsIfAttemptingToPutAtNegativeSequenceNumber() {
        assertThatThrownBy(() -> coordinationStore.putUnlessValueExists(-1, VALUE_1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Only positive sequence numbers are supported");
    }

    @Test
    public void throwsIfAttemptingToPutTwice() {
        coordinationStore.putUnlessValueExists(SEQUENCE_NUMBER_1, VALUE_1);
        assertThatThrownBy(() -> coordinationStore.putUnlessValueExists(SEQUENCE_NUMBER_1, VALUE_2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("The coordination store failed a putUnlessExists. This is unexpected"
                        + " as it implies timestamps may have been reused, or a writer to the store behaved badly.");
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
        byte[] otherCoordinationKey = PtBytes.toBytes("bbbbb");
        CoordinationStore<String> otherCoordinationStore
                = KeyValueServiceCoordinationStore.create(
                        ObjectMappers.newServerObjectMapper(),
                        keyValueService,
                        otherCoordinationKey,
                        timestampSequence::incrementAndGet,
                        String.class,
                        false);
        coordinationStore.transformAgreedValue(unused -> VALUE_1);
        otherCoordinationStore.transformAgreedValue(unused -> VALUE_2);
        assertThat(coordinationStore.getAgreedValue().get().value()).contains(VALUE_1);
        assertThat(otherCoordinationStore.getAgreedValue().get().value()).contains(VALUE_2);
    }
}
