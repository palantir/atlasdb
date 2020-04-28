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

package com.palantir.atlasdb.http.v2;

import java.util.Optional;

import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.okhttp.HostEventsSink;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.refreshable.Refreshable;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class AtlasDialogueServiceCreator implements AutoCloseable {
    static final StaticClientConfiguration EMPTY_STATIC_CONFIG =
            StaticClientConfiguration.builder().build();

    private final Refreshable<Optional<ServerListConfig>> clientConfig;
    private final DialogueClientFactory dialogueClientFactory;

    public AtlasDialogueServiceCreator(
            Refreshable<ServerListConfig> clientConfig,
            TaggedMetricRegistry taggedMetricRegistry,
            UserAgent userAgent,
            HostEventsSink hostEventsSink) {
        this.clientConfig = clientConfig.map(c -> c.hasAtLeastOneServer() ? Optional.of(c) : Optional.empty());
        this.dialogueClientFactory = new DialogueClientFactory(this.clientConfig, taggedMetricRegistry, userAgent,
                hostEventsSink);
    }

    public <T> T client(Class<T> serviceClass) {
        return client(serviceClass, EMPTY_STATIC_CONFIG);
    }

    // TODO(forozco): support multiple services
    public <T> T client(Class<T> serviceClass, StaticClientConfiguration staticConfig) {
        SafeArg<String> serviceClassSafeArg = SafeArg.of("serviceClass", serviceClass.getName());
        Preconditions.checkArgument(
                serviceClass.isInterface(), "serviceClass must be an interface", serviceClassSafeArg);

        if (DialogueClientFactory.isDialogue(serviceClass)) {
            return dialogueClientFactory.dialogueClient(serviceClass, staticConfig);
        } else {
            return dialogueClientFactory.jaxrsClient(serviceClass, staticConfig);
        }
    }

    @Override
    public void close() {
        dialogueClientFactory.close();
    }
}
