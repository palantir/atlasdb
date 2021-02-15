/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.paxos;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.palantir.common.annotation.Immutable;

@Immutable
public class BooleanPaxosResponse implements PaxosResponse {
    private static final long serialVersionUID = 1L;

    private final boolean ack;

    public BooleanPaxosResponse(@JsonProperty("successful") boolean ack) {
        this.ack = ack;
    }

    @Override
    public boolean isSuccessful() {
        return ack;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        BooleanPaxosResponse that = (BooleanPaxosResponse) other;
        return ack == that.ack;
    }

    @Override
    public int hashCode() {
        return Boolean.hashCode(ack);
    }

    @Override
    public String toString() {
        return "BooleanPaxosResponse [" + "ack=" + ack + ']';
    }
}
