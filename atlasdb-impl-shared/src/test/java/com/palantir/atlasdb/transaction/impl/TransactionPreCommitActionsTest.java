/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.common.api.timelock.TimestampLeaseName;
import com.palantir.atlasdb.transaction.impl.TransactionPreCommitActions.PerLeaseActions;
import com.palantir.atlasdb.transaction.impl.TransactionPreCommitActions.PreCommitAction;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

class TransactionPreCommitActionsTest {
    private static final TimestampLeaseName LEASE_NAME_1 = TimestampLeaseName.of("lease_name_1");
    private static final TimestampLeaseName LEASE_NAME_2 = TimestampLeaseName.of("lease_name_@");

    private final TransactionPreCommitActions allActions = new TransactionPreCommitActions();

    @Test
    void test() {
        Consumer<LongSupplier> action1 = _timestamps -> {};
        Consumer<LongSupplier> action2 = _timestamps -> {};
        Consumer<LongSupplier> action3 = _timestamps -> {};

        allActions.addPreCommitAction(LEASE_NAME_1, 5, action1);
        allActions.addPreCommitAction(LEASE_NAME_1, 10, action2);
        allActions.addPreCommitAction(LEASE_NAME_2, 10, action3);

        Map<TimestampLeaseName, PerLeaseActions> actions = allActions.getActions();
        assertThat(actions).hasSize(2);
        assertThat(actions.get(LEASE_NAME_1).timestampCount).isEqualTo(15);
        assertThat(actions.get(LEASE_NAME_1).preCommitActions)
                .containsExactlyInAnyOrder(new PreCommitAction(action2, 10), new PreCommitAction(action1, 5));
        assertThat(actions.get(LEASE_NAME_2).timestampCount).isEqualTo(10);
        assertThat(actions.get(LEASE_NAME_2).preCommitActions).containsExactly(new PreCommitAction(action3, 10));
    }
}
