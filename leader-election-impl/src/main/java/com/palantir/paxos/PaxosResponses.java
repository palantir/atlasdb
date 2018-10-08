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

import com.google.common.base.Predicate;
import com.palantir.paxos.persistence.generated.remoting.PaxosAcceptorPersistence;

public final class PaxosResponses {
    private PaxosResponses() {}

    public static Predicate<PaxosResponse> isSuccessfulPredicate() {
        return response -> response != null && response.isSuccessful();
    }

    public static PaxosAcceptorPersistence.PaxosResponse toProto(PaxosResponse result) {
        return PaxosAcceptorPersistence.PaxosResponse.newBuilder()
                .setAck(result.isSuccessful())
                .build();
    }

    public static PaxosResponse fromProto(PaxosAcceptorPersistence.PaxosResponse proto) {
        boolean ack = proto.getAck();
        return new PaxosResponseImpl(ack);
    }
}
