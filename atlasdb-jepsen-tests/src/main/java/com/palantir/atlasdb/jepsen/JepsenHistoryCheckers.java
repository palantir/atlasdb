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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.jepsen.events.Checker;
import com.palantir.atlasdb.jepsen.lock.IsolatedProcessCorrectnessChecker;
import com.palantir.atlasdb.jepsen.lock.LockCorrectnessChecker;
import com.palantir.atlasdb.jepsen.lock.RefreshCorrectnessChecker;
import com.palantir.atlasdb.jepsen.timestamp.MonotonicChecker;
import com.palantir.atlasdb.jepsen.timestamp.NonOverlappingReadsMonotonicChecker;
import com.palantir.atlasdb.jepsen.timestamp.UniquenessChecker;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;


public final class JepsenHistoryCheckers {
    private JepsenHistoryCheckers() {
        // utility
    }

    @VisibleForTesting
    static final List<Supplier<Checker>> TIMESTAMP_CHECKERS = ImmutableList.of(
            MonotonicChecker::new,
            NonOverlappingReadsMonotonicChecker::new,
            UniquenessChecker::new);

    @VisibleForTesting
    static final List<Supplier<Checker>> LOCK_CHECKERS = ImmutableList.of(
            () -> new PartitionByInvokeNameCheckerHelper(IsolatedProcessCorrectnessChecker::new),
            () -> new PartitionByInvokeNameCheckerHelper(LockCorrectnessChecker::new),
            () -> new PartitionByInvokeNameCheckerHelper(RefreshCorrectnessChecker::new));

    public static JepsenHistoryChecker createWithTimestampCheckers() {
        return createWithCheckers(TIMESTAMP_CHECKERS);
    }

    public static JepsenHistoryChecker createWithLockCheckers() {
        return createWithCheckers(LOCK_CHECKERS);
    }

    @VisibleForTesting
    static JepsenHistoryChecker createWithCheckers(List<Supplier<Checker>> checkers) {
        return new JepsenHistoryChecker(
                checkers.stream()
                        .map(Supplier::get)
                        .collect(Collectors.toList()));
    }
}
