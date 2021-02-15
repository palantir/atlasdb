/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.http.v2.ConjureJavaRuntimeTargetFactory;
import com.palantir.atlasdb.http.v2.DialogueShimFactory;
import com.palantir.atlasdb.http.v2.FastFailoverProxy;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.proxy.ReplaceIfExceptionMatchingProxy;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.dialogue.Channel;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

public final class AtlasDbHttpClients {

    private AtlasDbHttpClients() {
        // Utility class
    }

    public static <T> T createProxy(
            Optional<TrustContext> trustContext, String uri, Class<T> type, AuxiliaryRemotingParameters parameters) {
        return wrapWithOkHttpBugHandling(type, () -> ConjureJavaRuntimeTargetFactory.DEFAULT
                .createProxy(trustContext, uri, type, parameters)
                .instance());
    }

    /**
     * Constructs an HTTP-invoking dynamic proxy for the specified type that will cycle through the list of supplied
     * endpoints after encountering an exception or connection failure, using the supplied SSL factory if it is
     * present. Also use the supplied the proxy selector to set the proxy on the clients if present.
     * <p>
     * Failover will continue to cycle through the supplied endpoint list indefinitely.
     */
    public static <T> T createProxyWithFailover(
            MetricsManager metricsManager,
            ServerListConfig serverListConfig,
            Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        Supplier<T> clientFactory = () -> instrument(
                metricsManager.getTaggedRegistry(),
                ConjureJavaRuntimeTargetFactory.DEFAULT.createProxyWithFailover(serverListConfig, type, parameters),
                type);
        return wrapWithOkHttpBugHandling(type, clientFactory);
    }

    public static <T> T createLiveReloadingProxyWithFailover(
            MetricsManager metricsManager,
            Supplier<ServerListConfig> serverListConfigSupplier,
            Class<T> type,
            AuxiliaryRemotingParameters clientParameters) {
        Supplier<T> clientFactory = () -> instrument(
                metricsManager.getTaggedRegistry(),
                ConjureJavaRuntimeTargetFactory.DEFAULT.createLiveReloadingProxyWithFailover(
                        serverListConfigSupplier, type, clientParameters),
                type);
        return wrapWithOkHttpBugHandling(type, clientFactory);
    }

    public static <T> T createDialogueProxy(TaggedMetricRegistry registry, Class<T> type, Channel channel) {
        return AtlasDbMetrics.instrumentWithTaggedMetrics(
                registry, type, createUninstrumentedDialogueProxy(type, channel));
    }

    public static <T> T createUninstrumentedDialogueProxy(Class<T> type, Channel channel) {
        T proxy = DialogueShimFactory.create(type, channel);
        return FastFailoverProxy.newProxyInstance(type, () -> proxy);
    }

    private static <T> T instrument(
            TaggedMetricRegistry taggedMetricRegistry, TargetFactory.InstanceAndVersion<T> client, Class<T> clazz) {
        return AtlasDbMetrics.instrumentWithTaggedMetrics(taggedMetricRegistry, clazz, client.instance());
    }

    /**
     * Returns a proxy which replaces the underlying proxy if:
     * 1. We see a SocketTimeoutException
     * 2. At most once every 20 minutes
     */
    private static <T> T wrapWithOkHttpBugHandling(Class<T> type, Supplier<T> supplier) {
        return ReplaceIfExceptionMatchingProxy.create(
                type, supplier, Duration.ofMinutes(20), AtlasDbHttpClients::isPossiblyOkHttpTimeoutBug);
    }

    @VisibleForTesting
    static boolean isPossiblyOkHttpTimeoutBug(Throwable throwable) {
        if (throwable instanceof SocketTimeoutException) {
            return true;
        }
        Throwable cause = throwable.getCause();
        return cause != null && isPossiblyOkHttpTimeoutBug(cause);
    }
}
