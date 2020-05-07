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

package com.palantir.atlasdb.http;

import java.io.Closeable;
import java.util.Optional;
import java.util.function.Supplier;

import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.conjure.java.config.ssl.TrustContext;

/**
 * Defines how an HTTP service is created. A service is defined as a cluster of zero or more nodes, where contacting
 * any of the nodes is legitimate (subject to redirects via 308s and 503s). If working with heterogeneous nodes and/or
 * broadcast is important (e.g. for Paxos Acceptor use cases), you should be very careful when using this class.
 *
 * Proxies must be resilient to servers repeatedly returning 308s that are large in number, but persist for only a short
 * duration. Furthermore, proxies should include in their {@link com.palantir.conjure.java.api.config.service.UserAgent}
 * information to allow client services to identify the protocol they are using to talk, via
 * {@link AtlasDbHttpProtocolVersion}.
 *
 * Proxies returned here should already be instrumented, if the user desires. Clients should not add direct
 * instrumentation to proxies returned by this class via {@link com.palantir.atlasdb.util.AtlasDbMetrics}.
 */
public interface HttpServiceCreationStrategy {
    <T> T createLiveReloadingProxyWithFailover(
            Class<T> type,
            AuxiliaryRemotingParameters clientParameters,
            String serviceName);
}
