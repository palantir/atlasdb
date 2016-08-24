/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.performance.benchmarks;

import com.palantir.atlasdb.performance.backend.AtlasDbServicesConnector;

/**
 * Created by dcohen on 8/23/16.
 */
public class KvsGetRangeUnsweptBenchmarks extends KvsGetRangeBenchmarks {

    private static final long NUM_UNSWEPT = 10;

    @Override
    public void setup(AtlasDbServicesConnector conn) {
        super.setupTable(conn);
        this.numRows = 1000;
        this.numRequests = 500;
        for (long storeTs = STORE_TS; storeTs < NUM_UNSWEPT; storeTs++) {
            System.out.println("storeTs: " + storeTs);
            storeData(storeTs);
        }
    }
}
