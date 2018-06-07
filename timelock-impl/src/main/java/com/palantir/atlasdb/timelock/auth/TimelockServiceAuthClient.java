/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.auth;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(as = ImmutableTimelockServiceAuthClient.class)
@JsonDeserialize(as = ImmutableTimelockServiceAuthClient.class)
@Value.Immutable
public abstract class TimelockServiceAuthClient {

    /**
     * Namespace for which requests should be authorized in Timelock server
     */
    public abstract String namespace();

    /**
     * Token that is expected with each request specific to the namespace above to Timelock server.
     * The requests that provide this token will be authorized for the namespace above.
     */
    public abstract String token();

}
