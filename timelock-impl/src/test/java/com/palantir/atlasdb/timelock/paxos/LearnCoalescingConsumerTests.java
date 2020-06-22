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

import static org.assertj.core.api.Assertions.entry;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosValue;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LearnCoalescingConsumerTests {

    private static final Client CLIENT_1 = Client.of("client-1");
    private static final Client CLIENT_2 = Client.of("client-2");

    @Mock
    private BatchPaxosLearner local;

    @Mock
    private BatchPaxosLearner remote;

    private final ExecutorService executor = Executors.newCachedThreadPool();

    @After
    public void tearDown() {
        executor.shutdown();
    }

    @Test
    public void canProcessBatch() {
        PaxosValue paxosValue1 = paxosValue(10);
        PaxosValue paxosValue2 = paxosValue(14);

        LearnCoalescingConsumer consumer = new LearnCoalescingConsumer(local, ImmutableList.of(remote), executor);
        consumer.apply(ImmutableSet.of(
                entry(CLIENT_1, paxosValue1),
                entry(CLIENT_1, paxosValue2),
                entry(CLIENT_2, paxosValue1)));

        SetMultimap<Client, PaxosValue> remoteRequest = ImmutableSetMultimap.<Client, PaxosValue>builder()
                .putAll(CLIENT_1, paxosValue1, paxosValue2)
                .put(CLIENT_2, paxosValue1)
                .build();

        verify(local).learn(remoteRequest);

        // since remote requests are fired off and we don't wait for responses, we have to verify that they're called,
        // *eventually*
        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> verify(remote).learn(remoteRequest));
    }

    private static PaxosValue paxosValue(long round) {
        return new PaxosValue(UUID.randomUUID().toString(), round, null);
    }
}
