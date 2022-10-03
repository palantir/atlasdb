/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.internalschema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.transaction.knowledge.KnownAbortedTransactionsImpl;
import org.immutables.value.Value;

/**
 * An {@link InternalSchemaInstallConfig} contains configurations options that can be used for controlling how the
 * internal schemas of an AtlasDB installation operate.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableInternalSchemaInstallConfig.class)
@JsonDeserialize(as = ImmutableInternalSchemaInstallConfig.class)
public abstract class InternalSchemaInstallConfig {

    /**
     * Used to configure the in-memory cache size for aborted transactions.
     * This configuration is only valid for transactions schema version 4.
     */
    @JsonProperty("versionFourAbortedTransactionsCacheSize")
    @Value.Default
    public int versionFourAbortedTransactionsCacheSize() {
        return KnownAbortedTransactionsImpl.MAXIMUM_CACHE_WEIGHT;
    }
}
