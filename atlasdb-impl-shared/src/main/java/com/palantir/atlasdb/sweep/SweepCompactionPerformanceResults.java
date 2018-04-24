/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep;

import org.immutables.value.Value;

@Value.Immutable
public abstract class SweepCompactionPerformanceResults {

    public abstract String tableName();

    public abstract long cellsDeleted();

    public abstract long cellsExamined();

    public abstract long elapsedMillis();

    public static ImmutableSweepCompactionPerformanceResults.Builder builder() {
        return ImmutableSweepCompactionPerformanceResults.builder();
    }
}
