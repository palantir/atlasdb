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
package com.palantir.atlasdb.jepsen;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.jepsen.events.Checker;
import java.util.List;
import java.util.function.Supplier;
import org.junit.Test;

public class JepsenHistoryCheckersTest {
    @Test
    public void canCreateWithTimestampCheckers() {
        JepsenHistoryChecker checker = JepsenHistoryCheckers.createWithTimestampCheckers();

        assertCheckerHasMatchingCheckers(JepsenHistoryCheckers.TIMESTAMP_CHECKERS, checker);
        assertThat(checker.getCheckers()).hasSize(JepsenHistoryCheckers.TIMESTAMP_CHECKERS.size());
    }

    @Test
    public void canCreateWithLockCheckers() {
        JepsenHistoryChecker checker = JepsenHistoryCheckers.createWithLockCheckers();

        assertCheckerHasMatchingCheckers(JepsenHistoryCheckers.LOCK_CHECKERS, checker);
        assertThat(checker.getCheckers()).hasSize(JepsenHistoryCheckers.LOCK_CHECKERS.size());
    }

    @Test
    public void createsDistinctCheckerInstances() {
        JepsenHistoryChecker checker1 = JepsenHistoryCheckers.createWithTimestampCheckers();
        JepsenHistoryChecker checker2 = JepsenHistoryCheckers.createWithTimestampCheckers();

        for (Checker checkerFromCheckerOne : checker1.getCheckers()) {
            for (Checker checkerFromCheckerTwo : checker2.getCheckers()) {
                assertThat(checkerFromCheckerOne).isNotSameAs(checkerFromCheckerTwo);
            }
        }
    }

    @Test
    public void canCreateWithAlternativeCheckers() {
        Checker dummyChecker = mock(Checker.class);
        JepsenHistoryChecker checker = JepsenHistoryCheckers.createWithCheckers(
                ImmutableList.of(() -> dummyChecker)
        );

        assertThat(checker.getCheckers()).containsExactly(dummyChecker);
    }

    private void assertCheckerHasMatchingCheckers(List<Supplier<Checker>> checkerList, JepsenHistoryChecker checker) {
        checkerList.forEach(
                supplier -> assertThat(checker.getCheckers()).hasAtLeastOneElementOfType(supplier.get().getClass()));
    }
}
