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

import com.palantir.atlasdb.common.api.timelock.TimestampLeaseName;
import com.palantir.atlasdb.timelock.api.GetMinLeasedTimestampRequests;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class MinLeasedTimestampGetterImpl implements MinLeasedTimestampGetter {
    private final NamespacedTimestampLeaseService delegate;

    public MinLeasedTimestampGetterImpl(NamespacedTimestampLeaseService delegate) {
        this.delegate = delegate;
    }

    @Override
    public Map<TimestampLeaseName, Long> getMinLeasedTimestamps(Set<TimestampLeaseName> timestampNames) {
        return delegate.getMinLeasedTimestamps(GetMinLeasedTimestampRequests.of(List.copyOf(timestampNames)))
                .get();
    }

    @Override
    public void close() {}
}
