/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.server;

import com.palantir.atlasdb.workload.config.WorkloadServerInstallConfig;
import com.palantir.atlasdb.workload.config.WorkloadServerRuntimeConfig;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStore;
import com.palantir.refreshable.Refreshable;
import java.util.concurrent.ExecutorService;

public class WorkloadServer {

    private final ExecutorService executorService;
    private final WorkloadServerInstallConfig installConfig;
    private final Refreshable<WorkloadServerRuntimeConfig> runtimeConfig;

    public WorkloadServer(
            WorkloadServerInstallConfig installConfig,
            Refreshable<WorkloadServerRuntimeConfig> runtimeConfig,
            ExecutorService executorService) {
        this.executorService = executorService;
        this.installConfig = installConfig;
        this.runtimeConfig = runtimeConfig;
    }

    public void run() {
        AtlasDbTransactionStore store = AtlasDbTransactionStore.create();
    }
}
