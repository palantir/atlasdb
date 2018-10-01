/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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
        givenGeneratorHasModulus(3);

        requestResiduesAndAssertBalanced(3);
    }

    @Test
    public void waitsToReuseResiduesIfNeeded() {
        givenGeneratorHasModulus(3);

        requestResiduesAndAssertBalanced(3);
        requestResiduesAndAssertBalanced(3);
    }

    @Test
    public void throwsIfUnmarkingUnmarkedResidue() {
        givenGeneratorHasModulus(3);

        assertThatThrownBy(() -> unmarkResidue(0)).satisfies(this::failureArisesFromUnmarkingResidues);
    }

    @Test
    public void canUnmarkMarkedResidues() {
        givenGeneratorHasModulus(3);

        requestResidues(3);
        unmarkResidue(0);
        unmarkResidue(1);
        unmarkResidue(2);
        // Not throwing is a success!
    }

    @Test
    public void cannotDoubleUnmarkMarkedResidues() {
        givenGeneratorHasModulus(3);

        requestResidues(3);

        unmarkResidue(0);
        assertThatThrownBy(() -> unmarkResidue(0)).satisfies(this::failureArisesFromUnmarkingResidues);
    }

    @Test
    public void reassignsUnmarkedResiduesIfTheyAreNowLeastReferenced() {
        givenGeneratorHasModulus(3);

        requestResiduesAndAssertBalanced(3);
        unmarkResidue(1);
        assertThat(requestResidues(1)).containsExactly(1);
    }

    @Test
    public void repeatedlyReassignsUnmarkedResidues() {
        givenGeneratorHasModulus(3);

        requestResiduesAndAssertBalanced(3);
        requestResiduesAndAssertBalanced(3);
        requestResiduesAndAssertBalanced(3);

        unmarkResidue(2);
        unmarkResidue(2);
        unmarkResidue(2);
        List<Integer> additionalResponses = requestResidues(6);

        assertThat(additionalResponses.subList(0, 3)).containsExactly(2, 2, 2);
        assertListBalanced(additionalResponses.subList(3, 6), 3);
    }

    private void givenGeneratorHasModulus(int modulus) {
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
