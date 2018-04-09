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

package com.palantir.atlasdb.sweep.priority;

import java.util.List;
import java.util.stream.Stream;

import org.immutables.value.Value;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.TableReference;

@Value.Immutable
public abstract class SweepPriorityOverrideConfig {
    /**
     * List of fully qualified table references (of the form namespace.tablename) that the background sweeper should
     * prioritise for sweeping. Tables that do not exist in the key-value service will be ignored.
     *
     * In a steady state, if any tables are configured in priorityList, then the background sweeper will select
     * randomly from these tables. Otherwise, the background sweeper will rely on existing heuristics for determining
     * which tables might be attractive candidates for sweeping.
     *
     * Live reloading: In all cases, consideration of the priority list only takes place between iterations of
     * sweep (so an existing iteration of sweep on a non-priority table will run to completion or failure before we
     * shift to a table on the priority list).
     */
    @Value.Default
    public List<String> priorityList() {
        return ImmutableList.of();
    }

    /**
     * List of fully qualified table references (of the form namespace.tablename) that the background sweeper
     * should not sweep. Tables that do not exist in the key-value service will be ignored. If all tables eligible
     * for sweep are blacklisted, then the background sweeper will report that there is nothing to sweep.
     *
     * Live reloading: If this parameter is live reloaded during an iteration of sweep and the current table being
     * swept is now blacklisted, the iteration of sweep will complete, and then another table (or no table) will be
     * selected for sweeping.
     */
    @Value.Default
    public List<String> blacklist() {
        return ImmutableList.of();
    }

    /**
     * Default configuration for the sweep priority override config is no overrides.
     */
    public static SweepPriorityOverrideConfig defaultConfig() {
        return ImmutableSweepPriorityOverrideConfig.builder().build();
    }

    //TODO(jkong): validate disjointness
    @Value.Check
    private void validateTableNames() {
        Stream.concat(priorityList().stream(), blacklist().stream()).forEach(tableName -> Preconditions.checkState(
                TableReference.isFullyQualifiedName(tableName),
                "%s is not a fully qualified table name",
                tableName));
    }
}
