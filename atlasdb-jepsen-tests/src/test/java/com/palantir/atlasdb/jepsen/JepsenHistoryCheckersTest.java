/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.jepsen;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class JepsenHistoryCheckersTest {
    @Test
    public void canCreateWithStandardCheckers() {
        JepsenHistoryChecker checker = JepsenHistoryCheckers.createWithStandardCheckers();

        JepsenHistoryCheckers.STANDARD_CHECKERS.forEach(
                supplier -> assertThat(checker.checkers).hasAtLeastOneElementOfType(supplier.get().getClass()));
        assertThat(checker.checkers).hasSize(JepsenHistoryCheckers.STANDARD_CHECKERS.size());
    }

    @Test
    public void createsDistinctCheckerInstances() {
        JepsenHistoryChecker checker1 = JepsenHistoryCheckers.createWithStandardCheckers();
        JepsenHistoryChecker checker2 = JepsenHistoryCheckers.createWithStandardCheckers();

        assertThat(checker1.checkers).allMatch(checker -> !checker2.checkers.contains(checker));
    }

    @Test
    public void canCreateWithAdditionalCheckers() {
        JepsenHistoryChecker checker = JepsenHistoryCheckers.createWithAdditionalCheckers(
                ImmutableList.of(NemesisResilienceChecker::new));

        assertThat(checker.checkers).hasAtLeastOneElementOfType(NemesisResilienceChecker.class);
    }

    @Test
    public void createWithAdditionalCheckersStillIncludesDefaultCheckers() {
        JepsenHistoryChecker checker = JepsenHistoryCheckers.createWithAdditionalCheckers(
                ImmutableList.of(NemesisResilienceChecker::new));

        JepsenHistoryCheckers.STANDARD_CHECKERS.forEach(
                supplier -> assertThat(checker.checkers).hasAtLeastOneElementOfType(supplier.get().getClass()));
    }
}
