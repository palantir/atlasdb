/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Map;
import java.util.Set;
import one.util.streamex.EntryStream;

public final class HostIdResults {
    private HostIdResults() {
        // utility
    }

    public static int getQuorumSize(int numHosts) {
        return (numHosts / 2) + 1;
    }

    public static <T> Map<T, Set<String>> getHostIdsFromSuccesses(Map<T, HostIdResult> resultMap) {
        return EntryStream.of(resultMap)
                .filterValues(result -> result.type() == HostIdResult.Type.SUCCESS)
                .mapValues(HostIdResult::hostIds)
                .toMap();
    }
}
