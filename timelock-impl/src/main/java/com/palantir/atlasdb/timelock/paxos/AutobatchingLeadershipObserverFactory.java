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

import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.common.streams.KeyedStream;
import com.palantir.leader.LeadershipObserver;
import com.palantir.paxos.Client;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public final class AutobatchingLeadershipObserverFactory implements Closeable {

    private final DisruptorAutobatcher<Map.Entry<Client, LeadershipEvent>, Void> leadershipEventProcessor;

    private AutobatchingLeadershipObserverFactory(
            DisruptorAutobatcher<Map.Entry<Client, LeadershipEvent>, Void> leadershipEventProcessor) {
        this.leadershipEventProcessor = leadershipEventProcessor;
    }

    public static AutobatchingLeadershipObserverFactory create(
            Consumer<SetMultimap<LeadershipEvent, Client>> consumer) {
        DisruptorAutobatcher<Map.Entry<Client, LeadershipEvent>, Void> leadershipEventProcessor = Autobatchers
                .<Map.Entry<Client, LeadershipEvent>, Void>independent(leadershipEvents ->
                        processEvents(consumer, leadershipEvents))
                .safeLoggablePurpose("leadership-observer")
                .build();

        return new AutobatchingLeadershipObserverFactory(leadershipEventProcessor);
    }

    public LeadershipObserver create(Client client) {
        return new AutobatchingMetricsDeregistrator(client);
    }

    private static void processEvents(
            Consumer<SetMultimap<LeadershipEvent, Client>> consumer,
            List<BatchElement<Map.Entry<Client, LeadershipEvent>, Void>> events) {

        SetMultimap<LeadershipEvent, Client> leadershipEventsToClients = KeyedStream
                .ofEntries(events.stream().map(BatchElement::argument))
                .mapEntries((client, leadershipEvent) -> Maps.immutableEntry(leadershipEvent, client))
                .collectToSetMultimap();

        consumer.accept(leadershipEventsToClients);

        // complete requests to unblock the autobatcher
        events.stream()
                .map(BatchElement::result)
                .forEach(future -> future.set(null));
    }

    @Override
    public void close() {
        leadershipEventProcessor.close();
    }

    public enum LeadershipEvent {
        GAINED_LEADERSHIP(true),
        LOST_LEADERSHIP(false);

        private final boolean isCurrentSuspectedLeader;

        LeadershipEvent(boolean isCurrentSuspectedLeader) {
            this.isCurrentSuspectedLeader = isCurrentSuspectedLeader;
        }

        public boolean isCurrentSuspectedLeader() {
            return isCurrentSuspectedLeader;
        }
    }

    private class AutobatchingMetricsDeregistrator implements LeadershipObserver {

        private final Client client;

        AutobatchingMetricsDeregistrator(Client client) {
            this.client = client;
        }

        @Override
        public void gainedLeadership() {
            leadershipEventProcessor.apply(Maps.immutableEntry(client, LeadershipEvent.GAINED_LEADERSHIP));
        }

        @Override
        public void lostLeadership() {
            leadershipEventProcessor.apply(Maps.immutableEntry(client, LeadershipEvent.LOST_LEADERSHIP));
        }
    }
}
