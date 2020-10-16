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

import com.palantir.common.annotation.Immutable;

@Immutable
public class PaxosResponseImpl implements PaxosResponse {
    private static final long serialVersionUID = 1L;

    final boolean ack;

    public PaxosResponseImpl(boolean ack) {
        this.ack = ack;
    }

    @Override
    public boolean isSuccessful() {
        return ack;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (ack ? 1231 : 1237);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PaxosResponseImpl other = (PaxosResponseImpl) obj;
        if (ack != other.ack) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "PaxosResponseImpl [ack=" + ack + "]";
    }
}
