/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.Autobatchers.SupplierKey;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.leader.PingableLeader;

public class BatchingPaxosPingableLeaderFactory {

    private final DisruptorAutobatcher<Client, Boolean> pingAutobatcher;
    private final DisruptorAutobatcher<SupplierKey, UUID> uuidAutobatcher;

    public BatchingPaxosPingableLeaderFactory(
            DisruptorAutobatcher<Client, Boolean> pingAutobatcher,
            DisruptorAutobatcher<SupplierKey, UUID> uuidAutobatcher) {
        this.pingAutobatcher = pingAutobatcher;
        this.uuidAutobatcher = uuidAutobatcher;
    }

    public static BatchingPaxosPingableLeaderFactory create(BatchPingableLeader batchPingableLeader) {
        DisruptorAutobatcher<Client, Boolean> pingAutobatcher =
                Autobatchers.coalescing(new PingCoalescingFunction(batchPingableLeader))
                        .safeLoggablePurpose("batch-paxos-pingable-leader.ping")
                        .build();

        DisruptorAutobatcher<SupplierKey, UUID> uuidAutobatcher =
                Autobatchers.coalescing(batchPingableLeader::uuid)
                        .safeLoggablePurpose("batch-paxos-pingable-leader.uuid")
                        .build();

        return new BatchingPaxosPingableLeaderFactory(pingAutobatcher, uuidAutobatcher);
    }

    public PingableLeader pingableLeaderFor(Client client) {
        return new BatchingPingableLeader(client);
    }

    private final class BatchingPingableLeader implements PingableLeader {

        private final Client client;

        private BatchingPingableLeader(Client client) {
            this.client = client;
        }

        @Override
        public boolean ping() {
            try {
                return pingAutobatcher.apply(client).get();
            } catch (InterruptedException e) {
                // TODO(fdesouza): handle
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new RuntimeException(e);
            }
        }

        @Override
        public String getUUID() {
            try {
                return uuidAutobatcher.apply(SupplierKey.INSTANCE).get().toString();
            } catch (InterruptedException e) {
                // TODO(fdesouza): handle
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new RuntimeException(e);
            }
        }
    }

}
