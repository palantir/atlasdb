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

package com.palantir.atlasdb.spi;

import com.palantir.atlasdb.cell.api.TransactionKeyValueServiceManager;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.refreshable.Refreshable;

/**
 * Factory for creating {@link TransactionKeyValueServiceManager} instances.
 *
 * Implementations have access to the coordination service to be able query their internal state at a particular
 * timestamp, as well as schedule changes to it at a future timestamp.
 *
 * AtlasDb will ensure that transactions that span state changes do not succeed and non-transactional workflows
 * are also retried.
 *
 * @param <T> type used for the coordination state. Should be a jackson-compatible POJO.
 */
public interface TransactionKeyValueServiceManagerFactory<T> {

    String getType();

    Class<T> coordinationValueClass();

    TransactionKeyValueServiceManager create(
            String namespace,
            DialogueClients.ReloadingFactory reloadingFactory,
            MetricsManager metricsManager,
            CoordinationService<T> coordinationService,
            KeyValueServiceManager keyValueServiceManager,
            KeyValueServiceConfig sourceInstall,
            TransactionKeyValueServiceConfig install,
            Refreshable<TransactionKeyValueServiceRuntimeConfig> runtime,
            boolean initializeAsync);
}
