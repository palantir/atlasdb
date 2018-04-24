/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock.config;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(as = ImmutableAsyncLockConfiguration.class)
@JsonDeserialize(as = ImmutableAsyncLockConfiguration.class)
@Value.Immutable
public abstract class AsyncLockConfiguration {
    /**
     * If enabled, creates and exposes the async lock service; otherwise, creates and exposes a synchronous
     * lock service.
     */
    @Value.Parameter
    @Value.Default
    public boolean useAsyncLockService() {
        return true;
    }

    /**
     * If enabled AND the async lock service is being used, prevents the legacy synchronous lock service from being
     * used for the AtlasDB transaction protocol.
     * Note that other uses of lock service (e.g. advisory locks) are still allowed.
     * This flag has no effect if the sync lock service is being used - this is not a 'safety check' then, as only one
     * lock service is being run in that case.
     *
     * This may be disabled to allow backwards compatibility with clients using versions of AtlasDB < 0.49.0.
     * However, rolling upgrades to versions up to and including 0.49.0 MUST NOT be done - doing them can result in
     * SEVERE DATA CORRUPTION for the client concerned.
     */
    @Value.Default
    @Value.Parameter
    public boolean disableLegacySafetyChecksWarningPotentialDataCorruption() {
        return false;
    }
}
