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
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;


@JsonTypeInfo(use = Id.CLASS, property = "@class")
public abstract class EndpointWithStatus {
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
        return write ? shouldUseForWrite() : shouldUseForRead();
    }

    public boolean shouldUseFor(boolean write, Collection<String> racksToBeExcluded) {
        return shouldUseFor(write) && !racksToBeExcluded.contains(get().rack());
    }

    public boolean shouldCountFor(boolean write) {
        return write ? shouldCountForWrite() : shouldCountForRead();
    }

    public boolean shouldCountFor(boolean write, Collection<String> racksToBeExcluded) {
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
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        EndpointWithStatus that = (EndpointWithStatus) obj;
        return Objects.equals(endpoint, that.endpoint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpoint);
    }
}
