/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.invariant;

import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.immutables.value.Value;

/**
 * A set of cells that may individually be valid, but considered together may violate some property that is expected
 * to hold across the group of cells.
 */
@Value.Immutable
public interface CrossCellInconsistency {
    Map<TableAndWorkloadCell, Optional<Integer>> inconsistentValues();

    @Value.Check
    default void consistsOfAtLeastTwoCells() {
        Preconditions.checkArgument(
                inconsistentValues().size() >= 2,
                "A cross-cell inconsistency must consist of at least two cells",
                SafeArg.of("values", inconsistentValues()));
    }

    static ImmutableCrossCellInconsistency.Builder builder() {
        return ImmutableCrossCellInconsistency.builder();
    }
}
