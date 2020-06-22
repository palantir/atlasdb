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

package com.palantir.atlasdb.timelock.transaction.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class DistributingModulusGeneratorTest {
    private DistributingModulusGenerator generator;

    @Test
    public void doesNotReuseResiduesIfNotNeeded() {
        setupGeneratorWithModulus(3);

        requestResiduesAndAssertEachUsedOnce(3);
    }

    @Test
    public void waitsToReuseResiduesIfNeeded() {
        setupGeneratorWithModulus(3);

        requestResiduesAndAssertEachUsedOnce(3);
        requestResiduesAndAssertEachUsedOnce(3);
    }

    @Test
    public void canUseDifferentNumberOfModuli() {
        setupGeneratorWithModulus(7);

        requestResiduesAndAssertEachUsedOnce(7);
    }

    @Test
    public void canUseResidueOne() {
        setupGeneratorWithModulus(1);

        IntStream.range(0, 50).forEach(unused -> requestResiduesAndAssertEachUsedOnce(1));
    }

    @Test
    public void throwsIfCreatedWithNonPositiveModuli() {
        assertThatThrownBy(() -> setupGeneratorWithModulus(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Modulus must be positive");
        assertThatThrownBy(() -> setupGeneratorWithModulus(-8))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Modulus must be positive");
    }

    @Test
    public void throwsIfUnmarkingUnmarkedResidue() {
        setupGeneratorWithModulus(3);

        assertThatThrownBy(() -> unmarkResidue(0))
                .satisfies(DistributingModulusGeneratorTest::failureArisesFromUnmarkingResidues);
    }

    @Test
    public void canUnmarkMarkedResidues() {
        setupGeneratorWithModulus(3);

        requestResidues(3);

        assertThatCode(() -> {
            unmarkResidue(0);
            unmarkResidue(1);
            unmarkResidue(2);
        })
                .as("unmarking all three residues does not throw")
                .doesNotThrowAnyException();
    }

    @Test
    public void cannotDoubleUnmarkMarkedResidues() {
        setupGeneratorWithModulus(3);

        requestResidues(3);

        unmarkResidue(0);
        assertThatThrownBy(() -> unmarkResidue(0))
                .satisfies(DistributingModulusGeneratorTest::failureArisesFromUnmarkingResidues);
    }

    @Test
    public void reassignsUnmarkedResiduesIfTheyAreNowLeastReferenced() {
        setupGeneratorWithModulus(3);

        requestResiduesAndAssertEachUsedOnce(3);
        unmarkResidue(1);
        assertThat(requestResidues(1)).containsExactly(1);
    }

    @Test
    public void repeatedlyReassignsUnmarkedResidues() {
        setupGeneratorWithModulus(3);

        requestResiduesAndAssertEachUsedOnce(3);
        requestResiduesAndAssertEachUsedOnce(3);
        requestResiduesAndAssertEachUsedOnce(3);

        unmarkResidue(2);
        unmarkResidue(2);
        unmarkResidue(2);

        List<Integer> additionalResponses = requestResidues(3);
        assertThat(additionalResponses).containsExactly(2, 2, 2);

        requestResiduesAndAssertEachUsedOnce(3);
    }

    @Test
    public void selectionOnFreshGeneratorsDoesNotHotSpot() {
        Map<Integer, Integer> frequencyMap = Maps.newHashMap();
        for (int trial = 0; trial < 20; trial++) {
            setupGeneratorWithModulus(16);
            int residue = Iterables.getOnlyElement(requestResidues(1));
            frequencyMap.compute(residue, (unusedKey, oldValue) -> oldValue == null ? 1 : oldValue + 1);
        }

        // Assuming uniform randomness, strobes once in 2^80 times, which we can live with
        assertThat(frequencyMap.size()).isGreaterThanOrEqualTo(2);
    }

    private void setupGeneratorWithModulus(int modulus) {
        generator = new DistributingModulusGenerator(modulus);
    }

    private List<Integer> requestResidues(int times) {
        return IntStream.range(0, times)
                .map(unused -> generator.getAndMarkResidue())
                .boxed()
                .collect(Collectors.toList());
    }

    private void requestResiduesAndAssertEachUsedOnce(int times) {
        List<Integer> responses = requestResidues(times);
        assertThat(responses).containsExactlyInAnyOrderElementsOf(
                IntStream.range(0, times).boxed().collect(Collectors.toList()));
    }

    private void unmarkResidue(int residue) {
        generator.unmarkResidue(residue);
    }

    private static void failureArisesFromUnmarkingResidues(Throwable th) {
        assertThat(th)
                .hasMessageContaining("Attempted to unmark residue")
                .hasMessageContaining("when it had no references");
    }
}
