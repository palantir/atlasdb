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

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.jepsen.events.Checker;

public final class JepsenHistoryCheckers {
    private JepsenHistoryCheckers() {
        // utility
    }

    @VisibleForTesting
    static final List<Supplier<Checker>> STANDARD_CHECKERS = ImmutableList.of(
            MonotonicChecker::new,
            NonOverlappingReadsMonotonicChecker::new,
            UniquenessChecker::new);

    @VisibleForTesting
    static final List<Supplier<Checker>> LIVENESS_CHECKERS = ImmutableList.of(
            NemesisResilienceChecker::new);

    public static JepsenHistoryChecker createWithStandardCheckers() {
        return createWithAdditionalCheckers(ImmutableList.of());
    }

    public static JepsenHistoryChecker createWithLivenessCheckers() {
        return createWithAdditionalCheckers(LIVENESS_CHECKERS);
    }

    @VisibleForTesting
    static JepsenHistoryChecker createWithAdditionalCheckers(List<Supplier<Checker>> additionalCheckers) {
        return new JepsenHistoryChecker(
                Stream.concat(STANDARD_CHECKERS.stream(), additionalCheckers.stream())
                        .map(Supplier::get)
                        .collect(Collectors.toList()));
    }
}
