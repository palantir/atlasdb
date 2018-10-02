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

package com.palantir.lock.logger;

import java.util.List;
import java.util.Map;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

public final class LockStateDebugger {

    public static void main(String[] args) {



        // Read in the yaml file

    }

    @JsonDeserialize(as = ImmutableYamlFileFormat.class)
    @JsonSerialize(as = ImmutableYamlFileFormat.class)
    @Value.Immutable
    private abstract class YamlFileFormat {

        @JsonProperty("OutstandingLockRequests")
        public abstract List<OutstandingLockRequestsForDescriptor> getOutstandingLockRequests();

        @JsonProperty("HeldLocks")
        public abstract Map<String, Object> getHeldLocks();
    }

    @JsonDeserialize(as = ImmutableOutstandingLockRequestsForDescriptor.class)
    @JsonSerialize(as = ImmutableOutstandingLockRequestsForDescriptor.class)
    @Value.Immutable
    private abstract class OutstandingLockRequestsForDescriptor {
        public abstract String getLockDescriptor();
        public abstract List<SimpleLockRequest> getLockRequests();
        public abstract int getLockRequestsCount();
    }
}
