/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.partition.status;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;

public class EndpointWithLeavingStatus extends BackfillableEndpointWithStatus {

    @JsonCreator
    public EndpointWithLeavingStatus(@JsonProperty("endpoint") KeyValueEndpoint endpoint) {
        super(endpoint);
    }

    @Override
    public boolean shouldUseForRead() {
        return true;
    }

    @Override
    public boolean shouldCountForRead() {
        return true;
    }

    @Override
    public boolean shouldUseForWrite() {
        // Since it is still being used for reads
        return true;
    }

    @Override
    public boolean shouldCountForWrite() {
        return false;
    }

    @Override
    public String toString() {
        return "EndpointWithLeavingStatus [endpoint=" + endpoint + "]";
    }

}
