/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosProposer;
import org.junit.Test;
import org.mockito.Answers;

public class PaxosResourcesFactoryTest {
    @Test
    public void individualTimestampServicesHaveDifferingProposers() {
        NetworkClientFactories.Factory<PaxosProposer> proposerFactory = PaxosResourcesFactory.getPaxosProposerFactory(
                TimelockPaxosMetrics.of(PaxosUseCase.TIMESTAMP, MetricsManagers.createForTests()),
                mock(NetworkClientFactories.class, Answers.RETURNS_DEEP_STUBS));
        Client client = Client.of("client");
        PaxosProposer proposer1 = proposerFactory.create(client);
        PaxosProposer proposer2 = proposerFactory.create(client);
        assertThat(proposer1.getUuid()).isNotEqualTo(proposer2.getUuid());
    }
}
