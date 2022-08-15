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

package com.palantir.atlasdb.atomic;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CasCassImitatingConsensusForgettingStoreTest {
    private static final Cell CELL = Cell.create(new byte[] {1}, new byte[] {0});
    private static final byte[] VALUE = PtBytes.toBytes("VAL");
    private static final byte[] VALUE_2 = PtBytes.toBytes("VAL2");
    // solution to (1-x)^4 = 0.5
    private static final double PROBABILITY_THROWING_ON_QUORUM_HALF = 0.16;
    ConsensusForgettingStore neverThrowing = new CasCassImitatingConsensusForgettingStore(0.0);
    CassandraImitatingConsensusForgettingStore sometimesThrowing =
            new CasCassImitatingConsensusForgettingStore(PROBABILITY_THROWING_ON_QUORUM_HALF);

    @Test
    public void trivialGet() throws ExecutionException, InterruptedException {
        assertThat(neverThrowing.get(CELL).get()).isEmpty();
    }

    @Test
    public void canGetAfterMark() throws ExecutionException, InterruptedException {
        neverThrowing.mark(CELL);
        assertThat(neverThrowing.get(CELL).get()).contains(CasCassImitatingConsensusForgettingStore.IN_PROGRESS_MARKER);
    }

    @Test
    public void canGetAfterUpdate() throws ExecutionException, InterruptedException {
        atomicUpdate(neverThrowing, CELL);
        assertThat(neverThrowing.get(CELL).get()).contains(VALUE);
    }

    @Test
    public void canGetAfterPut() throws ExecutionException, InterruptedException {
        neverThrowing.put(CELL, VALUE);
        assertThat(neverThrowing.get(CELL).get()).hasValue(VALUE);
    }

    @Test
    public void putOverwritesUpdate() throws ExecutionException, InterruptedException {
        atomicUpdate(neverThrowing, CELL);
        neverThrowing.put(CELL, VALUE_2);
        assertThat(neverThrowing.get(CELL).get()).hasValue(VALUE_2);
    }

    @Test
    public void cannotDoAtomicUpdateTwice() {
        neverThrowing.mark(CELL);

        // First atomic update
        neverThrowing.atomicUpdate(CELL, VALUE);

        // Second atomic update
        assertThatThrownBy(() -> neverThrowing.atomicUpdate(CELL, VALUE))
                .isInstanceOf(CheckAndSetException.class)
                .satisfies(exception -> {
                    CheckAndSetException checkAndSetException = (CheckAndSetException) exception;
                    assertThat(checkAndSetException.getKey()).isEqualTo(CELL);
                    assertThat(checkAndSetException.getActualValues()).containsExactly(VALUE);
                });
    }

    @Test
    public void cannotAtomicUpdateAfterMark() throws ExecutionException, InterruptedException {
        assertThatThrownBy(() -> neverThrowing.atomicUpdate(CELL, VALUE)).isInstanceOf(CheckAndSetException.class);
        assertThat(neverThrowing.get(CELL).get()).isEmpty();
    }

    @Test
    public void canAtomicUpdateAfterMark() throws ExecutionException, InterruptedException {
        neverThrowing.mark(CELL);
        neverThrowing.atomicUpdate(CELL, VALUE);
        assertThat(neverThrowing.get(CELL).get()).hasValue(VALUE);
    }

    @Test
    public void canTouchAfterAtomicUpdate() throws ExecutionException, InterruptedException {
        atomicUpdate(neverThrowing, CELL);
        neverThrowing.checkAndTouch(CELL, VALUE);
        assertThat(neverThrowing.get(CELL).get()).hasValue(VALUE);
    }

    @Test
    public void cannotTouchWhenNotMatching() {
        assertThatThrownBy(() -> neverThrowing.checkAndTouch(CELL, VALUE))
                .isInstanceOf(CheckAndSetException.class)
                .satisfies(exception -> assertThat(((CheckAndSetException) exception).getActualValues())
                        .isEmpty());

        atomicUpdate(neverThrowing, CELL);
        assertThatThrownBy(() -> neverThrowing.checkAndTouch(CELL, VALUE_2))
                .isInstanceOf(CheckAndSetException.class)
                .satisfies(exception -> assertThat(((CheckAndSetException) exception).getActualValues())
                        .containsExactly(VALUE));
    }

    @Test
    public void testPartialFailuresInMarking() throws ExecutionException, InterruptedException {
        int numberOfSuccessfulUpdates = 0;
        int numberOfNothingPresent = 0;
        int numberOfValuePresentAfterFailure = 0;
        for (int i = 0; i < 100; i++) {
            Cell cell = Cell.create(PtBytes.toBytes(i), PtBytes.toBytes(i));
            try {
                sometimesThrowing.mark(cell);
                numberOfSuccessfulUpdates++;
                sometimesThrowing.setProbabilityOfFailure(0.0);
                assertThat(sometimesThrowing.get(cell).get()).hasValue(CasCassImitatingConsensusForgettingStore.IN_PROGRESS_MARKER);
                sometimesThrowing.setProbabilityOfFailure(PROBABILITY_THROWING_ON_QUORUM_HALF);
            } catch (RuntimeException e) {
                sometimesThrowing.setProbabilityOfFailure(0.0);
                Optional<byte[]> actualValue = sometimesThrowing.get(cell).get();
                if (actualValue.isEmpty()) {
                    numberOfNothingPresent++;
                } else {
                    assertThat(actualValue).hasValue(CasCassImitatingConsensusForgettingStore.IN_PROGRESS_MARKER);
                    numberOfValuePresentAfterFailure++;
                }
                sometimesThrowing.setProbabilityOfFailure(PROBABILITY_THROWING_ON_QUORUM_HALF);
            }
        }
        // expected half succeed
        assertThat(numberOfSuccessfulUpdates).isBetween(30, 70);
        // too lazy to calculate exactly, rough estimates
        assertThat(numberOfNothingPresent).isBetween(5, 40);
        assertThat(numberOfValuePresentAfterFailure).isBetween(10, 55);
    }

    private void atomicUpdate(ConsensusForgettingStore store, Cell cell) {
        store.mark(cell);
        store.atomicUpdate(cell, VALUE);
    }
}
