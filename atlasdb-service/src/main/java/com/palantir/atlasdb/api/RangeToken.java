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
package com.palantir.atlasdb.api;

import javax.annotation.Nullable;

import com.palantir.logsafe.Preconditions;

/*
 * <pre>
 * {
 *   "data": &lt;results>,
 *   "next": &lt;nextRange>
 * }
 * </pre>
 */
public class RangeToken {
    private final TableRowResult results;
    private final @Nullable TableRange nextRange;

    public RangeToken(TableRowResult results,
                      @Nullable TableRange nextRange) {
        this.results = Preconditions.checkNotNull(results, "results must not be null!");
        this.nextRange = nextRange;
    }

    public TableRowResult getResults() {
        return results;
    }

    public boolean hasMoreResults() {
        return nextRange != null;
    }

    public @Nullable TableRange getNextRange() {
        return nextRange;
    }

    @Override
    public String toString() {
        return "RangeToken [results=" + results + ", nextRange=" + nextRange + "]";
    }
}
