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

import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.timelock.api.ConjureLeaderTimesRequest;
import com.palantir.atlasdb.timelock.api.ConjureMultiNamespaceTimelockService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.tokens.auth.AuthHeader;
import com.palantir.tracing.Observability;

public final class MultiNamespaceTimelockService {

    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer omitted");

    private final DisruptorAutobatcher<Namespace, LeaderTime> leaderTimeAutobatcher;

    private MultiNamespaceTimelockService(
            ConjureMultiNamespaceTimelockService service) {
        leaderTimeAutobatcher = Autobatchers.coalescing(
                (CoalescingRequestFunction<Namespace, LeaderTime>) request -> service.leaderTime(AUTH_HEADER,
                        ConjureLeaderTimesRequest.builder()
                                .requests(request)
                                .build()).getResponses()).observability(Observability.SAMPLE)
                .safeLoggablePurpose("leader-time-autobatcher")
                .build();
    }

    public LeaderTime leaderTime(Namespace namespace) {
        return Futures.getUnchecked(leaderTimeAutobatcher.apply(namespace));
    }

    public interface Factory {
        MultiNamespaceTimelockService create(Supplier<ConjureMultiNamespaceTimelockService> serviceSupplier);
    }

    // So much butchering
    public static Factory factory() {
        return new Factory() {
            private Optional<MultiNamespaceTimelockService> service = Optional.empty();

            @Override
            public synchronized MultiNamespaceTimelockService create(
                    Supplier<ConjureMultiNamespaceTimelockService> serviceSupplier) {
                if (!service.isPresent()) {
                    service = Optional.of(new MultiNamespaceTimelockService(serviceSupplier.get()));
                }
                return service.get();
            }
        };
    }
}
