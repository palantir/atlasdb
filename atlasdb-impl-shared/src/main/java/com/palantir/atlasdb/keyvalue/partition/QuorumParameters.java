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
package com.palantir.atlasdb.keyvalue.partition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.palantir.common.annotation.Immutable;

@Immutable
public final class QuorumParameters {
    final int replicationFactor;
    final int readFactor;
    final int writeFactor;

    @Immutable
    public final static class QuorumRequestParameters {
        final int replicationFator;
        final int successFactor;

        public int getReplicationFator() {
            return replicationFator;
        }

        /**
         * After this many successes a success can be concluded
         */
        public int getSuccessFactor() {
            return successFactor;
        }

        /**
         * After this many failures a failure can be concluded
         */
        public int getFailureFactor() {
            return replicationFator - successFactor + 1;
        }

        private QuorumRequestParameters(int replicationFactor, int successFactor) {
            this.replicationFator = replicationFactor;
            this.successFactor = successFactor;
        }
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public int getReadFactor() {
        return readFactor;
    }

    public int getWriteFactor() {
        return writeFactor;
    }

    @JsonIgnore
    public QuorumRequestParameters getReadRequestParameters() {
        return new QuorumRequestParameters(replicationFactor, readFactor);
    }

    @JsonIgnore
    public QuorumRequestParameters getWriteRequestParameters() {
        return new QuorumRequestParameters(replicationFactor, writeFactor);
    }

    @JsonIgnore
    public QuorumRequestParameters getNoFailureRequestParameters() {
        return new QuorumRequestParameters(replicationFactor, replicationFactor);
    }

    @JsonCreator
    public QuorumParameters(@JsonProperty("replicationFactor") int replicationFactor,
                            @JsonProperty("readFactor") int readFactor,
                            @JsonProperty("writeFactor") int writeFactor) {
        Preconditions.checkArgument(readFactor + writeFactor > replicationFactor);
        this.replicationFactor = replicationFactor;
        this.readFactor = readFactor;
        this.writeFactor = writeFactor;
    }

    @Override
    public String toString() {
        return "QuorumParameters [replicationFactor=" + replicationFactor + ", readFactor="
                + readFactor + ", writeFactor=" + writeFactor + "]";
    }

}
