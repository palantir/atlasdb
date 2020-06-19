/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api.watch;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.palantir.atlasdb.internalschema.persistence.InternalSchemaMetadataPayloadCodecTest;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.ImmutableTransactionUpdate;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.TransactionUpdate;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;

public class LockWatchEventCacheIntegrationTest2 {
    private static final String TABLE = "table";
    private static final LockDescriptor DESCRIPTOR = AtlasRowLockDescriptor.of(TABLE, new byte[] {1});
    private static final LockDescriptor DESCRIPTOR_2 = AtlasRowLockDescriptor.of(TABLE, new byte[] {2});
    private static final LockDescriptor DESCRIPTOR_3 = AtlasRowLockDescriptor.of(TABLE, new byte[] {3});
    private static final LockWatchReferences.LockWatchReference REFERENCE = LockWatchReferences.entireTable("table");
    private static final UUID COMMIT_UUID = UUID.fromString("203fcd7a-b3d7-4c2a-9d2c-3d61cde1ba59");
    private static final LockToken COMMIT_TOKEN = LockToken.of(COMMIT_UUID);

    private static final LockWatchEvent WATCH_EVENT =
            LockWatchCreatedEvent.builder(ImmutableSet.of(REFERENCE), ImmutableSet.of(DESCRIPTOR_2)).build(4L);
    private static final LockWatchEvent UNLOCK_EVENT = UnlockEvent.builder(ImmutableSet.of(DESCRIPTOR_2)).build(5L);
    private static final LockWatchEvent LOCK_EVENT =
            LockEvent.builder(ImmutableSet.of(DESCRIPTOR_3), COMMIT_TOKEN).build(6L);
    private static final UUID EVENT2_UUID = UUID.fromString("888fcd7a-b3d7-4d2a-9d2c-3d61cde1ba44");
    private static final LockWatchEvent LOCK_EVENT_2 =
            LockEvent.builder(ImmutableSet.of(DESCRIPTOR), LockToken.of(EVENT2_UUID)).build(10L);

    private static final UUID LEADER = UUID.fromString("470c855e-f77b-44df-b56a-14d3df085dbc");
    private static final LockWatchStateUpdate SNAPSHOT =
            LockWatchStateUpdate.snapshot(LEADER, 3L, ImmutableSet.of(DESCRIPTOR_2), ImmutableSet.of());
    private static final LockWatchStateUpdate SUCCESS =
            LockWatchStateUpdate.success(LEADER, 6L, ImmutableList.of(WATCH_EVENT, UNLOCK_EVENT, LOCK_EVENT));
    private static final long START_TS = 1L;
    private static final Set<TransactionUpdate> COMMIT_UPDATE = ImmutableSet.of(
            ImmutableTransactionUpdate.builder().startTs(START_TS).commitTs(5L).writesToken(COMMIT_TOKEN).build());

//    private final MetricsManager metricsManager = new MetricsManager(
//            new MetricRegistry(),
//            new DefaultTaggedMetricRegistry(),
//            unused -> false);

    private enum Mode {
        DEV,
        CI;

        boolean isDev() {
            return this.equals(Mode.DEV);
        }
    }

    private static final Mode MODE = Mode.DEV;
    private static final String BASE = "src/test/resources/lockwatch-event-cache-output/";

    private LockWatchEventCache eventCache;

    @Rule
    public TestName name = new TestName();

    @Before
    public void before() {
        eventCache = new LockWatchEventCacheImpl(LockWatchEventLogImpl.create());
    }

    private void verifyStage(int part) {
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        try {
            Path path = Paths.get(BASE + name.getMethodName() + "/event-cache-" + part + ".json");

            if (MODE.isDev()) {
                mapper.writeValue(path.toFile(), eventCache);
            } else {
                String ourJson = mapper.writeValueAsString(eventCache);
                String theirJson = new String(Files.readAllBytes(path), Charset.defaultCharset());
                assertThat(ourJson).isEqualTo(theirJson);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void getCommitUpdateDoesNotContainCommitLocks() {
        verifyStage(0);

        Set<Long> timestamps = ImmutableSet.of(START_TS);
        eventCache.processStartTransactionsUpdate(timestamps, SNAPSHOT);
        verifyStage(1);

        eventCache.processGetCommitTimestampsUpdate(COMMIT_UPDATE, SUCCESS);
        verifyStage(2);
    }

    @Test
    public void getEventsForTransactionsReturnsSnapshotWithOldEvents() {
        verifyStage(0);

        Set<Long> timestamps = ImmutableSet.of(START_TS);
        eventCache.processStartTransactionsUpdate(timestamps, SNAPSHOT);
        eventCache.processGetCommitTimestampsUpdate(COMMIT_UPDATE, SUCCESS);
        eventCache.removeTransactionStateFromCache(START_TS);
        verifyStage(1);

        LockWatchStateUpdate success2 = LockWatchStateUpdate.success(LEADER, 10L, ImmutableList.of(LOCK_EVENT_2));
        Set<Long> timestamps2 = ImmutableSet.of(16L);
        eventCache.processStartTransactionsUpdate(timestamps2, success2);
        verifyStage(2);

        TransactionsLockWatchUpdate results = eventCache.getUpdateForTransactions(timestamps2, Optional.empty());
        assertThat(results.clearCache()).isTrue();
        assertThat(results.startTsToSequence()).containsExactlyInAnyOrderEntriesOf(
                ImmutableMap.of(16L, IdentifiedVersion.of(LEADER, 10L)));
        assertThat(results.events()).containsExactly(
                LockWatchCreatedEvent.builder(ImmutableSet.of(REFERENCE), ImmutableSet.of(DESCRIPTOR_3)).build(6L),
                LOCK_EVENT_2);
    }

    private static final class CommitUpdateVisitor implements CommitUpdate.Visitor<Set<LockDescriptor>> {

        @Override
        public Set<LockDescriptor> invalidateAll() {
            return ImmutableSet.of();
        }

        @Override
        public Set<LockDescriptor> invalidateSome(Set<LockDescriptor> invalidatedLocks) {
            return invalidatedLocks;
        }
    }
}
