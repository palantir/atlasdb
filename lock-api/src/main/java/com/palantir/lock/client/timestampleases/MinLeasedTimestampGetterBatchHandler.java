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

package com.palantir.lock.client.timestampleases;

import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.lock.client.InternalMultiClientConjureTimelockService;
import java.util.Map;
import java.util.Set;

final class MinLeasedTimestampGetterBatchHandler
        implements CoalescingRequestFunction<MinLeasedTimestampGetterBatchParams, Long> {
    private final InternalMultiClientConjureTimelockService delegate;

    MinLeasedTimestampGetterBatchHandler(InternalMultiClientConjureTimelockService delegate) {
        this.delegate = delegate;
    }

    @Override
    public Map<MinLeasedTimestampGetterBatchParams, Long> apply(Set<MinLeasedTimestampGetterBatchParams> request) {
        // TODO(aalouane): implement
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
