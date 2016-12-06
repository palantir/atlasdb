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

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.jepsen.events.Checker;

public class JepsenHistoryCheckers {
    private static final List<Supplier<Checker>> DEFAULT_CHECKER_SUPPLIERS = ImmutableList.of(
            MonotonicChecker::new,
            NonOverlappingReadsMonotonicChecker::new,
            UniquenessChecker::new);

    private JepsenHistoryCheckers() {
        // utility class
    }

    public static JepsenHistoryChecker createWithDefaultCheckers() {
        return createCheckerWithAdditionalSuppliers(ImmutableList.of());
    }

    public static JepsenHistoryChecker createCheckerForPartitionRandomHalves() {
        return createCheckerWithAdditionalSuppliers(ImmutableList.of(NemesisLivenessChecker::new));
    }

    private static JepsenHistoryChecker createCheckerWithAdditionalSuppliers(List<Supplier<Checker>> suppliers) {
        return new JepsenHistoryChecker(
                Stream.concat(DEFAULT_CHECKER_SUPPLIERS.stream(), suppliers.stream())
                        .map(Supplier::get)
                        .collect(Collectors.toList()));
    }
}
