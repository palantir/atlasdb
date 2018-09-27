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

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.junit.Test;

import com.google.common.collect.Lists;

public class DistributingModulusGeneratorTest {
    private DistributingModulusGenerator generator;
    private List<Integer> answers = Lists.newArrayList();
    private Optional<Exception> failure = Optional.empty();

    @Test
    public void doesNotReuseResiduesIfNotNeeded() {
        givenGeneratorHasModulus(3);

        whenModuliAreRequested(3);

        thenOperationSucceeds();
        thenResponsesSatisfy(responses -> assertThat(responses).containsExactlyInAnyOrder(0, 1, 2));
    }

    @Test
    public void waitsToReuseResiduesIfNeeded() {
        givenGeneratorHasModulus(3);

        whenModuliAreRequested(6);

        thenOperationSucceeds();
        thenResponsesSatisfy(responses -> {
            assertThat(responses).hasSize(6);
            assertThat(responses.subList(0, 3)).containsExactlyInAnyOrder(0, 1, 2);
            assertThat(responses.subList(3, 6)).containsExactlyInAnyOrder(0, 1, 2);
        });
    }

    @Test
    public void throwsIfUnmarkingUnmarkedResidue() {
        givenGeneratorHasModulus(3);

        whenResidueIsUnmarked(0);

        thenOperationFails();
    }

    @Test
    public void canUnmarkMarkedResidues() {
        givenGeneratorHasModulus(3);

        whenModuliAreRequested(3);
        whenResidueIsUnmarked(0);
        whenResidueIsUnmarked(1);
        whenResidueIsUnmarked(2);

        thenOperationSucceeds();
    }

    @Test
    public void cannotDoubleUnmarkMarkedResidues() {
        givenGeneratorHasModulus(3);

        whenModuliAreRequested(3);
        whenResidueIsUnmarked(0);
        whenResidueIsUnmarked(0);

        thenOperationFails();
    }

    @Test
    public void reassignsUnmarkedResiduesIfTheyAreNowLeastReferenced() {
        givenGeneratorHasModulus(3);

        whenModuliAreRequested(3);
        whenResidueIsUnmarked(1);
        whenModuliAreRequested(1);

        thenOperationSucceeds();
        thenResponsesSatisfy(responses -> {
            assertThat(responses).hasSize(4);
            assertThat(responses.subList(0, 3)).containsExactlyInAnyOrder(0, 1, 2);
            assertThat(responses.get(3)).isEqualTo(1);
        });
    }

    @Test
    public void repeatedlyReassignsUnmarkedResidues() {
        givenGeneratorHasModulus(3);

        whenModuliAreRequested(9);
        whenResidueIsUnmarked(2);
        whenResidueIsUnmarked(2);
        whenResidueIsUnmarked(2);
        whenModuliAreRequested(6);

        thenOperationSucceeds();
        thenResponsesSatisfy(responses -> {
            assertThat(responses).hasSize(15);
            assertThat(responses.get(9)).isEqualTo(2);
            assertThat(responses.get(10)).isEqualTo(2);
            assertThat(responses.get(11)).isEqualTo(2);
            assertThat(responses.subList(12, 15)).containsExactlyInAnyOrder(0, 1, 2);
        });
    }

    private void givenGeneratorHasModulus(int modulus) {
        generator = DistributingModulusGenerator.create(modulus);
    }

    private void whenModuliAreRequested(int times) {
        IntStream.range(0, times)
                .forEach(unused -> answers.add(generator.getAndMarkResidue()));
    }

    private void whenResidueIsUnmarked(int residue) {
        try {
            generator.unmarkResidue(residue);
        } catch (Exception e) {
            failure = Optional.of(e);
        }
    }

    private void thenOperationSucceeds() {
        assertThat(failure).isEmpty();
    }

    private void thenOperationFails() {
        assertThat(failure).isPresent();
    }

    private void thenResponsesSatisfy(Consumer<List<Integer>> assertion) {
        assertion.accept(answers);
    }
}
