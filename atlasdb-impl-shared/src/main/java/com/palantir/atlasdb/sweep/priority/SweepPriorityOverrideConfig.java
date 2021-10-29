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
package com.palantir.atlasdb.sweep.priority;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.immutables.value.Value;

@JsonSerialize(as = ImmutableSweepPriorityOverrideConfig.class)
@JsonDeserialize(as = ImmutableSweepPriorityOverrideConfig.class)
@Value.Immutable
public abstract class SweepPriorityOverrideConfig {
    /**
     * Fully qualified table references (of the form namespace.tablename) that the background sweeper should
     * prioritise for sweeping. Tables that do not exist in the key-value service will be ignored.
     *
     * In a steady state, if any tables are configured in priorityTables, then the background sweeper will select
     * randomly from these tables. Otherwise, the background sweeper will rely on existing heuristics for determining
     * which tables might be attractive candidates for sweeping.
     *
     * Live reloading: In all cases, consideration of the priority tables only takes place between iterations of
     * sweep (so an existing iteration of sweep on a non-priority table will run to completion or failure before we
     * shift to a table on the priority list).
     */
    @Value.Default
    public Set<String> priorityTables() {
        return ImmutableSet.of();
    }

    /**
     * Derived from {@link SweepPriorityOverrideConfig#priorityTables()}, but returns a list, which is useful for
     * fast random selection of priority tables. There are no guarantees on the order of elements in this list,
     * though it is guaranteed that on the same {@link SweepPriorityOverrideConfig} object, the list elements are
     * in a consistent order.
     */
    @Value.Derived
    public List<String> priorityTablesAsList() {
        return ImmutableList.copyOf(priorityTables());
    }

    /**
     * Fully qualified table references (of the form namespace.tablename) that the background sweeper
     * should not sweep. Tables that do not exist in the key-value service will be ignored. If all tables eligible
     * for sweep are blacklisted, then the background sweeper will report that there is nothing to sweep.
     *
     * Live reloading: If this parameter is live reloaded during an iteration of sweep and the current table being
     * swept is now blacklisted, the iteration of sweep will complete, and then another table (or no table) will be
     * selected for sweeping.
     */
    @Value.Default
    public Set<String> blacklistTables() {
        return ImmutableSet.of();
    }

    /**
     * If true, sweep of thorough tables will intentionally skip ~1% of entries. This is useful for tables that switched
     * from conservative to thorough sweep, because doing a full sweep may result in too many consecutive deletes in the
     * KVS layer which can cause issues (in particular can cause OOMs or other failures in Cassandra)
     */
    @Value.Default
    public boolean runLeakySweepOnThoroughTables() {
        return false;
    }

    /**
     * Default configuration for the sweep priority override config is no overrides.
     */
    public static SweepPriorityOverrideConfig defaultConfig() {
        return ImmutableSweepPriorityOverrideConfig.builder().build();
    }

    @Value.Check
    void validateTableNames() {
        Stream.concat(priorityTables().stream(), blacklistTables().stream())
                .forEach(tableName -> Preconditions.checkState(
                        TableReference.isFullyQualifiedName(tableName),
                        "%s is not a fully qualified table name",
                        tableName));
    }

    @Value.Check
    void validatePriorityTablesAndBlacklistTablesAreDisjoint() {
        Preconditions.checkState(
                Sets.intersection(priorityTables(), blacklistTables()).isEmpty(),
                "The priority and blacklist tables should not have any overlap, but found %s",
                Sets.intersection(priorityTables(), blacklistTables()));
    }
}
