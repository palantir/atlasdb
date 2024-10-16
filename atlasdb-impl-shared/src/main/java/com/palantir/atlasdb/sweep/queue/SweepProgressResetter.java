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

package com.palantir.atlasdb.sweep.queue;

import com.palantir.atlasdb.table.description.SweeperStrategy;
import java.util.Set;

public interface SweepProgressResetter {
    /**
     * Resets sweep progress for all shards and buckets for all provided strategies. If you are running your service in
     * an HA configuration, then the reset is only guaranteed to be complete after this has executed across all nodes
     * of your service.
     */
    void resetProgress(Set<SweeperStrategy> strategies);
}
