/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.atlasdb.timelock.api.MultiClientConjureTimelockService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.tokens.auth.AuthHeader;
import java.util.Map;
import java.util.Set;

public class LeaderTimeCoalescingConsumer implements CoalescingRequestFunction<Namespace, LeaderTime> {
    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer omitted");
    private final MultiClientConjureTimelockService delegate;

    private LeaderTimeCoalescingConsumer(MultiClientConjureTimelockService delegate) {
        this.delegate = delegate;
    }

    public static LeaderTimeCoalescingConsumer create(MultiClientConjureTimelockService delegate) {
        return new LeaderTimeCoalescingConsumer(delegate);
    }

    @Override
    public Map<Namespace, LeaderTime> apply(Set<Namespace> namespaces) {
        return delegate.leaderTimes(AUTH_HEADER, namespaces).getNamespaceWiseLeaderTimes();
    }
}
