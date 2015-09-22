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

import java.util.Collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.palantir.atlasdb.keyvalue.partition.PartitionedKeyValueConstants;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;


@JsonTypeInfo(use=Id.CLASS, property="@class")
public abstract class EndpointWithStatus {

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((endpoint == null) ? 0 : endpoint.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EndpointWithStatus other = (EndpointWithStatus) obj;
        if (endpoint == null) {
            if (other.endpoint != null)
                return false;
        } else if (!endpoint.equals(other.endpoint))
            return false;
        return true;
    }

    @JsonProperty("endpoint")
    final KeyValueEndpoint endpoint;

    public final KeyValueEndpoint get() {
        return endpoint;
    }

    @JsonCreator
    public EndpointWithStatus(@JsonProperty("endpoint") KeyValueEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    public abstract boolean shouldUseForRead();
    public abstract boolean shouldCountForRead();
    public abstract boolean shouldUseForWrite();
    public abstract boolean shouldCountForWrite();

    public boolean shouldUseFor(boolean write) {
        if (write) {
            return shouldUseForWrite();
        }
        return shouldUseForRead();
    }

    public boolean shouldUseFor(boolean write, Collection<String> racksToBeExcluded) {
        if (PartitionedKeyValueConstants.NO_RACK.equals(get().rack())) {
            return shouldUseFor(write);
        }
        return shouldUseFor(write) && !racksToBeExcluded.contains(get().rack());
    }

    public boolean shouldCountFor(boolean write) {
        if (write) {
            return shouldCountForWrite();
        }
        return shouldCountForRead();
    }

    public boolean shouldCountFor(boolean write, Collection<String> racksToBeExcluded) {
        if (PartitionedKeyValueConstants.NO_RACK.equals(get().rack())) {
            return shouldCountFor(write);
        }
        return shouldCountFor(write) && !racksToBeExcluded.contains(get().rack());
    }

    public EndpointWithLeavingStatus asLeaving() {
        return new EndpointWithLeavingStatus(get());
    }

    public EndpointWithJoiningStatus asJoining() {
        return new EndpointWithJoiningStatus(get());
    }

    public EndpointWithNormalStatus asNormal() {
        return new EndpointWithNormalStatus(get());
    }

    @Override
    public String toString() {
        return "EndpointWithStatus [endpoint=" + endpoint + "]";
    }
}
