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

package com.palantir.atlasdb.internalschema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * An {@link InternalSchemaRuntimeConfig} contains information that can be used for controlling how the internal schema
 * of an AtlasDB installation operates.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableInternalSchemaRuntimeConfig.class)
@JsonDeserialize(as = ImmutableInternalSchemaRuntimeConfig.class)
public abstract class InternalSchemaRuntimeConfig {
    /**
     * If specified, attempts to install the provided transactions schema version to this AtlasDB installation.
     * This is expected to be supported by the version of AtlasDB deployed on this service node.
     *
     * If unspecified, this AtlasDB installation should not attempt to install any new schema versions for
     * transaction persistence.
     */
    public abstract Optional<Integer> targetTransactionsSchemaVersion();

    /**
     * If specified, reads of STAGING values are treated as definitively committed when reading a transactions3 value.
     * This addresses a problem faced at stacks where users have particularly high read throughput and the CAS performed
     * to confirm that a quorum of nodes backs the relevant STAGING value can create contention.
     *
     * This is intended as a workaround. Enabling this setting will mean that the correctness guarantees that
     * transactions3 provides will not be realised (though we believe that enabling this setting will not worsen the
     * risk of inconsistency beyond that experienced by transactions2).
     *
     * Do not enable this flag without consulting with the AtlasDB team.
     */
    @JsonProperty("acceptStagingReadsOnVersionThreeDangerousIKnowWhatIAmDoing")
    @Value.Default
    public boolean acceptStagingReadsOnVersionThree() {
        return true;
    }

    @Value.Check
    public void check() {
        targetTransactionsSchemaVersion()
                .ifPresent(version -> Preconditions.checkState(
                        TransactionConstants.SUPPORTED_TRANSACTIONS_SCHEMA_VERSIONS.contains(version),
                        "Unrecognised transactions schema version.",
                        SafeArg.of("configuredVersion", version),
                        SafeArg.of("supportedVersions", TransactionConstants.SUPPORTED_TRANSACTIONS_SCHEMA_VERSIONS)));
    }
}
