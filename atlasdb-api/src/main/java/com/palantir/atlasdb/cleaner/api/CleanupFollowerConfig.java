/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.cleaner.api;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Contains information that may be useful for the execution of cleanup tasks.
 */
@JsonSerialize(as = ImmutableCleanupFollowerConfig.class)
@JsonDeserialize(as = ImmutableCleanupFollowerConfig.class)
@Value.Immutable
public interface CleanupFollowerConfig {
    /**
     * Enables cleanup of unreferenced streams in stream store cleanup tasks.
     *
     * This codepath has historically experienced bugs which the AtlasDB team has had difficulty debugging, even though
     * on a conceptual level it is permissible to clean up unreferenced streams from stream stores, and the AtlasDB team
     * is not aware of any concrete bugs in their implementation. This option must ONLY be used if your usage of streams
     * is not strictly required for correctness; that is, in the event of a stream going missing, there will be no data
     * loss, and your application will handle the situation correctly.
     *
     * To repeat: if your use case is unable to handle a stream going missing when you don't expect it, DO NOT ENABLE
     * THIS OPTION. There is a risk of SEVERE DATA CORRUPTION(TM) if you do so.
     */
    @Value.Default
    default boolean dangerousRiskOfDataCorruptionEnableCleanupOfUnreferencedStreamsInStreamStoreCleanupTasks() {
        return false;
    }
}
