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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import com.google.common.collect.Iterables;

public class DistributingModulusGeneratorTest {
    private DistributingModulusGenerator generator;

    @Test
    public void doesNotReuseResiduesIfNotNeeded() {
        setupGeneratorWithModulus(3);

        requestResiduesAndAssertBalanced(3);
    }

    @Test
    public void waitsToReuseResiduesIfNeeded() {
        setupGeneratorWithModulus(3);

        requestResiduesAndAssertBalanced(3);
        requestResiduesAndAssertBalanced(3);
    }

    @Test
    public void canUseDifferentNumberOfModuli() {
        setupGeneratorWithModulus(7);

        requestResiduesAndAssertBalanced(7);
    }

    @Test
    public void canUseResidueOne() {
        setupGeneratorWithModulus(1);

        IntStream.range(0, 50).forEach(unused -> requestResiduesAndAssertBalanced(1));
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

        assertThatThrownBy(() -> unmarkResidue(0)).satisfies(this::failureArisesFromUnmarkingResidues);
    }

    @Test
    public void canUnmarkMarkedResidues() {
        setupGeneratorWithModulus(3);

        requestResidues(3);
        unmarkResidue(0);
        unmarkResidue(1);
        unmarkResidue(2);
        // Not throwing is a success!
    }

    @Test
    public void cannotDoubleUnmarkMarkedResidues() {
        setupGeneratorWithModulus(3);

        requestResidues(3);

        unmarkResidue(0);
        assertThatThrownBy(() -> unmarkResidue(0)).satisfies(this::failureArisesFromUnmarkingResidues);
    }

    @Test
    public void reassignsUnmarkedResiduesIfTheyAreNowLeastReferenced() {
        setupGeneratorWithModulus(3);

        requestResiduesAndAssertBalanced(3);
        unmarkResidue(1);
        assertThat(requestResidues(1)).containsExactly(1);
    }

    @Test
    public void repeatedlyReassignsUnmarkedResidues() {
        setupGeneratorWithModulus(3);

        requestResiduesAndAssertBalanced(3);
        requestResiduesAndAssertBalanced(3);
        requestResiduesAndAssertBalanced(3);

        unmarkResidue(2);
        unmarkResidue(2);
        unmarkResidue(2);

        List<Integer> additionalResponses = requestResidues(3);
        assertThat(additionalResponses).containsExactly(2, 2, 2);

        requestResiduesAndAssertBalanced(3);
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

    private void requestResiduesAndAssertBalanced(int times) {
        assertListBalanced(requestResidues(times), times);
    }

    /**
     * Asserts that, starting from the beginning of the list, each full modulus number of elements has the integers
     * from 0 to (modulus - 1) once each. No guarantees are made on the remainder of the list.
     */
    private void assertListBalanced(List<Integer> responses, int modulus) {
        List<Integer> targets = IntStream.range(0, modulus).boxed().collect(Collectors.toList());
        for (List<Integer> subList : Iterables.partition(responses, modulus)) {
            if (subList.size() == modulus) {
                assertThat(subList).hasSameElementsAs(targets);
            }
        }
    }

    private void unmarkResidue(int residue) {
        generator.unmarkResidue(residue);
    }

    private void failureArisesFromUnmarkingResidues(Throwable th) {
        assertThat(th)
                .hasMessageContaining("Attempted to unmark residue")
                .hasMessageContaining("when it had no references");
    }
}
