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

import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import org.junit.Test;

public final class VersionedTableViewTest {
    @Test
    public void putStoresTable() {
        VersionedTableView<Long, Long> tableView = new VersionedTableView<>();
        tableView.put(1L, HashMap.of(10L, 20L));
        assertThat(tableView.getLatestTableView().getSnapshot().toJavaMap())
                .containsExactlyEntriesOf(java.util.Map.of(10L, 20L));
    }

    @Test
    public void getLatestTableViewFetchesLargestVersion() {
        VersionedTableView<Long, Long> tableView = new VersionedTableView<>();
        Map<Long, Long> expectedLastView = HashMap.of(30L, 40L);
        tableView.put(1L, HashMap.of(10L, 20L));
        tableView.put(2L, HashMap.of(20L, 30L));
        tableView.put(3L, HashMap.of(30L, 40L));
        assertThat(tableView.getLatestTableView().getSnapshot()).isEqualTo(expectedLastView);
    }

    @Test
    public void getViewFetchesViewBeforeTimestamp() {
        VersionedTableView<Long, Long> tableView = new VersionedTableView<>();
        Map<Long, Long> expectedView = HashMap.of(10L, 20L);
        tableView.put(1L, expectedView);
        tableView.put(2L, HashMap.of(20L, 30L));
        assertThat(tableView.getView(2L).getSnapshot()).isEqualTo(expectedView);
    }
}
