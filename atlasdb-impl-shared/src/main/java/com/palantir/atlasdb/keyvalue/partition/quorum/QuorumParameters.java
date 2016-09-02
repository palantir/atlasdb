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
package com.palantir.atlasdb.keyvalue.partition.quorum;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.palantir.common.annotation.Immutable;

@Immutable
public final class QuorumParameters {
    private final int replicationFactor;
    private final int readFactor;
    private final int writeFactor;

    @Immutable
    public static final class QuorumRequestParameters {
        private final int replicationFactor;
        private final int successFactor;

        private QuorumRequestParameters(int replicationFactor, int successFactor) {
            this.replicationFactor = replicationFactor;
            this.successFactor = successFactor;
        }

        public int getReplicationFactor() {
            return replicationFactor;
        }

        /**
         * After this many successes a success can be concluded.
         */
        public int getSuccessFactor() {
            return successFactor;
        }

        /**
         * After this many failures a failure can be concluded.
         */
        public int getFailureFactor() {
            return replicationFactor - successFactor + 1;
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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        QuorumParameters that = (QuorumParameters) obj;
        return replicationFactor == that.replicationFactor
                && readFactor == that.readFactor
                && writeFactor == that.writeFactor;
    }

    @Override
    public int hashCode() {
        return Objects.hash(replicationFactor, readFactor, writeFactor);
    }
}
