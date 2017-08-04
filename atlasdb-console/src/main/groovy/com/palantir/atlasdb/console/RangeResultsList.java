/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.console;

import javax.annotation.Nullable;

import com.palantir.atlasdb.api.RangeResult;
import com.palantir.atlasdb.api.TableRange;

public class RangeResultsList {
    private final Iterable<RangeResult> results;

    private final @Nullable TableRange nextRange;
    public RangeResultsList(Iterable<RangeResult> results, @Nullable TableRange nextRange) {
        this.results = results;
        this.nextRange = nextRange;
    }

    public Iterable<RangeResult> getResults() {
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
        return "RangeResultsList [results=" + results + ", nextRange=" + nextRange + "]";
    }

}
