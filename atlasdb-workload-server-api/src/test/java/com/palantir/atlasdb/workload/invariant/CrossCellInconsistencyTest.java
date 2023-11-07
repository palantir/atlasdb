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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class CrossCellInconsistencyTest {
    // The constants in WorkloadTestHelper are not accessible from this package.
    private static final TableAndWorkloadCell TABLE_WORKLOAD_CELL_ONE =
            TableAndWorkloadCell.of("foo", ImmutableWorkloadCell.of(1, 2));
    private static final TableAndWorkloadCell TABLE_WORKLOAD_CELL_TWO =
            TableAndWorkloadCell.of("foo", ImmutableWorkloadCell.of(3, 4));

    @Test
    public void cannotBeCreatedWithoutCells() {
        assertThatThrownBy(() -> CrossCellInconsistency.builder().build())
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("A cross-cell inconsistency must consist of at least two cells");
    }

    @Test
    public void cannotBeCreatedWithJustOneCell() {
        assertThatThrownBy(() -> CrossCellInconsistency.builder()
                        .putInconsistentValues(TABLE_WORKLOAD_CELL_ONE, Optional.of(3))
                        .build())
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("A cross-cell inconsistency must consist of at least two cells");
    }

    @Test
    public void canBeCreatedWithTwoCells() {
        CrossCellInconsistency violation = CrossCellInconsistency.builder()
                .putInconsistentValues(TABLE_WORKLOAD_CELL_ONE, Optional.of(3))
                .putInconsistentValues(TABLE_WORKLOAD_CELL_TWO, Optional.empty())
                .build();
        assertThat(violation.inconsistentValues()).containsOnlyKeys(TABLE_WORKLOAD_CELL_ONE, TABLE_WORKLOAD_CELL_TWO);
    }
}
