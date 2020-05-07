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

import java.lang.reflect.Method;
import java.util.Optional;

import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.http.v2.AtlasDbDialogueFactories;
import com.palantir.atlasdb.http.v2.FastFailoverProxy;
import com.palantir.dialogue.Channel;
import com.palantir.dialogue.ConjureRuntime;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.refreshable.Refreshable;

public class DialogueHttpServiceCreationStrategy implements HttpServiceCreationStrategy {
    private final Refreshable<ServerListConfig> serverListConfigRefreshable;
    private final DialogueClients.ReloadingFactory reloadingFactory;
    private final HttpServiceCreationStrategy fallback;

    public DialogueHttpServiceCreationStrategy(
            Refreshable<ServerListConfig> serverListConfigRefreshable,
            DialogueClients.ReloadingFactory reloadingFactory,
            HttpServiceCreationStrategy fallback) {
        this.serverListConfigRefreshable = serverListConfigRefreshable;
        this.reloadingFactory = reloadingFactory;
        this.fallback = fallback;
    }

    @Override
    public <T> T createLiveReloadingProxyWithFailover(Class<T> type, AuxiliaryRemotingParameters clientParameters,
            String serviceName) {
        if (isDialogue(type)) {
            T rawProxy = AtlasDbDialogueFactories.decorate(
                    reloadingFactory, serverListConfigRefreshable, clientParameters, serviceName)
                    .get(type, serviceName);
            return FastFailoverProxy.newProxyInstance(type, () -> rawProxy); // Dialogue does its own instrumentation.
        }
        return fallback.createLiveReloadingProxyWithFailover(type, clientParameters, serviceName);
    }

    private static boolean isDialogue(Class<?> serviceInterface) {
        return getStaticOfMethod(serviceInterface).isPresent();
    }

    private static Optional<Method> getStaticOfMethod(Class<?> dialogueInterface) {
        try {
            return Optional.ofNullable(dialogueInterface.getMethod("of", Channel.class, ConjureRuntime.class));
        } catch (NoSuchMethodException e) {
            return Optional.empty();
        }
    }
}
