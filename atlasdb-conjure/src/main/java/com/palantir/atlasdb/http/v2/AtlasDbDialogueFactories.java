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

import java.time.Duration;
import java.util.Map;

import com.palantir.conjure.java.client.config.ClientConfiguration;
import com.palantir.conjure.java.client.config.NodeSelectionStrategy;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.refreshable.Refreshable;

public final class AtlasDbDialogueFactories {
    private static final Duration STANDARD_FAILED_URL_COOLDOWN = Duration.ofMillis(100);

    private AtlasDbDialogueFactories() {
        // no
    }

    public static DialogueClients.ReloadingFactory decorateForFailoverServices(
            DialogueClients.ReloadingFactory baseFactory,
            Refreshable<Map<String, RemoteServiceConfiguration>> serviceToRemoteConfiguration) {
        return baseFactory.reloading(serviceToRemoteConfiguration.map(
                DialogueClientOptions::toServicesConfigBlock))
                .withFailedUrlCooldown(STANDARD_FAILED_URL_COOLDOWN)
                .withNodeSelectionStrategy(NodeSelectionStrategy.PIN_UNTIL_ERROR_WITHOUT_RESHUFFLE)
                .withClientQoS(ClientConfiguration.ClientQoS.DANGEROUS_DISABLE_SYMPATHETIC_CLIENT_QOS);
    }
}