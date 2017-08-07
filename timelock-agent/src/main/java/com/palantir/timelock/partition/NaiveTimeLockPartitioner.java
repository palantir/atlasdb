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

package com.palantir.timelock.partition;

import java.util.List;

public class NaiveTimeLockPartitioner implements TimeLockPartitioner {
    @Override
    public Assignment partition(List<String> clients, List<String> hosts, long seed) {
        return Assignment.builder()
                .addMapping("foundry-catalog", "http://localhost:8421")
                .addMapping("foundry-catalog", "http://localhost:8425")
                .addMapping("foundry-catalog", "http://localhost:8427")
                .addMapping("small1", "http://localhost:8421")
                .addMapping("small1", "http://localhost:8423")
                .addMapping("small1", "http://localhost:8429")
                .addMapping("small2", "http://localhost:8423")
                .addMapping("small2", "http://localhost:8425")
                .addMapping("small2", "http://localhost:8429")
                .addMapping("small3", "http://localhost:8423")
                .addMapping("small3", "http://localhost:8427")
                .addMapping("small3", "http://localhost:8429")
                .build();
    }
}
