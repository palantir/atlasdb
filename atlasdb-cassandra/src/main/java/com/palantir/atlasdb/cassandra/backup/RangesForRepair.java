/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.cassandra.backup;

import com.google.common.collect.RangeSet;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import java.net.InetSocketAddress;
import java.util.Map;

@SuppressWarnings("UnstableApiUsage")
public class RangesForRepair {
    private final Map<InetSocketAddress, RangeSet<LightweightOppToken>> tokenMap;

    public RangesForRepair(Map<InetSocketAddress, RangeSet<LightweightOppToken>> tokenMap) {
        this.tokenMap = tokenMap;
    }

    public Map<InetSocketAddress, RangeSet<LightweightOppToken>> asMap() {
        return tokenMap;
    }

    public RangeSet<LightweightOppToken> get(InetSocketAddress address) {
        return tokenMap.get(address);
    }
}
