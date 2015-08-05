/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.schema;

public class MigrateTimer {
    private long numBatches;
    private long totalTimeMillis;

    public MigrateTimer() {
        numBatches = 0;
        totalTimeMillis = 0;
    }

    public synchronized void update(long millis) {
        numBatches += 1;
        totalTimeMillis += millis;
    }

    public synchronized long getTimePerBatchInMillis() {
        if (numBatches > 0) {
            return totalTimeMillis / numBatches;
        }
        return 0;
    }

    public synchronized long getNumBatches() {
        return numBatches;
    }
}
