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
package com.palantir.lock.logger;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockCollections;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.SortedLockCollection;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.impl.ClientAwareReadWriteLock;
import com.palantir.lock.impl.LockClientIndices;
import com.palantir.lock.impl.LockServerLock;
import com.palantir.lock.impl.LockServiceImpl;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.assertj.core.util.Strings;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

public class LockServiceStateLoggerTest {

    private static final ConcurrentMap<HeldLocksToken, LockServiceImpl.HeldLocks<HeldLocksToken>> heldLocksTokenMap =
            new MapMaker().makeMap();

    private static final SetMultimap<LockClient, LockRequest> outstandingLockRequestMultimap =
            Multimaps.synchronizedSetMultimap(HashMultimap.create());

    private static final Map<LockDescriptor, ClientAwareReadWriteLock> syncStateMap = new HashMap<>();

    private static final LockDescriptor DESCRIPTOR_1 = StringLockDescriptor.of("logger-lock");
    private static final LockDescriptor DESCRIPTOR_2 = StringLockDescriptor.of("logger-AAA");

    @BeforeClass
    public static void setUp() throws Exception {
        LockServiceTestUtils.cleanUpLogStateDir();

        LockClient clientA = LockClient.of("Client A");
        LockClient clientB = LockClient.of("Client B");

        LockRequest request1 = LockRequest.builder(
                        LockCollections.of(ImmutableSortedMap.of(DESCRIPTOR_1, LockMode.WRITE)))
                .blockForAtMost(SimpleTimeDuration.of(1000, TimeUnit.MILLISECONDS))
                .build();
        LockRequest request2 = LockRequest.builder(
                        LockCollections.of(ImmutableSortedMap.of(DESCRIPTOR_2, LockMode.WRITE)))
                .blockForAtMost(SimpleTimeDuration.of(1000, TimeUnit.MILLISECONDS))
                .build();

        outstandingLockRequestMultimap.put(clientA, request1);
        outstandingLockRequestMultimap.put(clientB, request2);
        outstandingLockRequestMultimap.put(clientA, request2);

        HeldLocksToken token = LockServiceTestUtils.getFakeHeldLocksToken(
                "client A", "Fake thread", new BigInteger("1"), "held-lock-1", "logger-lock");
        HeldLocksToken token2 = LockServiceTestUtils.getFakeHeldLocksToken(
                "client B", "Fake thread 2", new BigInteger("2"), "held-lock-2", "held-lock-3");

        heldLocksTokenMap.putIfAbsent(token, LockServiceImpl.HeldLocks.of(token, LockCollections.of()));
        heldLocksTokenMap.putIfAbsent(token2, LockServiceImpl.HeldLocks.of(token2, LockCollections.of()));

        LockServerLock lock1 = new LockServerLock(DESCRIPTOR_1, new LockClientIndices());
        syncStateMap.put(DESCRIPTOR_1, lock1);
        LockServerLock lock2 = new LockServerLock(DESCRIPTOR_2, new LockClientIndices());
        lock2.get(clientA, LockMode.WRITE).lock();
        syncStateMap.put(DESCRIPTOR_2, lock2);

        LockServiceStateLogger logger = new LockServiceStateLogger(
                heldLocksTokenMap,
                outstandingLockRequestMultimap,
                syncStateMap,
                LockServiceTestUtils.TEST_LOG_STATE_DIR);
        logger.logLocks();
    }

    @Test
    public void testFilesExist() {
        List<File> files = LockServiceTestUtils.logStateDirFiles();

        assertThat(files.stream()
                        .filter(file -> file.getName().startsWith(LockServiceStateLogger.DESCRIPTORS_FILE_PREFIX))
                        .count())
                .describedAs("Unexpected number of descriptor files")
                .isEqualTo(1);

        assertThat(files.stream()
                        .filter(file -> file.getName().startsWith(LockServiceStateLogger.LOCKSTATE_FILE_PREFIX))
                        .count())
                .describedAs("Unexpected number of lock state files")
                .isEqualTo(1);

        assertThat(files.stream()
                        .filter(file -> file.getName().startsWith(LockServiceStateLogger.SYNC_STATE_FILE_PREFIX))
                        .count())
                .describedAs("Unexpected number of lock sync state files")
                .isEqualTo(1);

        assertThat(files.stream()
                        .filter(file ->
                                file.getName().startsWith(LockServiceStateLogger.SYNTHESIZED_REQUEST_STATE_FILE_PREFIX))
                        .count())
                .describedAs("Unexpected number of synthesized request state files")
                .isEqualTo(1);
    }

    @Test
    public void testDescriptors() throws Exception {
        List<File> files = LockServiceTestUtils.logStateDirFiles();

        Optional<File> descriptorsFile = files.stream()
                .filter(file -> file.getName().startsWith(LockServiceStateLogger.DESCRIPTORS_FILE_PREFIX))
                .findFirst();

        Map<String, String> descriptorsMap =
                (Map<String, String>) new Yaml().loadAs(new FileInputStream(descriptorsFile.get()), Map.class);

        Set<LockDescriptor> allDescriptors = getAllDescriptors();

        for (LockDescriptor descriptor : allDescriptors) {
            assertThat(descriptorsMap.values().stream()
                            .anyMatch(descriptorFromFile -> descriptorFromFile.equals(descriptor.toString())))
                    .describedAs("Existing descriptor can't be found in dumped descriptors")
                    .isTrue();
        }
    }

    @Test
    public void testLockState() throws Exception {
        List<File> files = LockServiceTestUtils.logStateDirFiles();

        Optional<File> lockStateFile = files.stream()
                .filter(file -> file.getName().startsWith(LockServiceStateLogger.LOCKSTATE_FILE_PREFIX))
                .findFirst();

        Iterable<Object> lockState = new Yaml().loadAll(new FileInputStream(lockStateFile.get()));

        for (Object ymlMap : lockState) {
            assertThat(ymlMap)
                    .describedAs("Lock state contains unrecognizable object")
                    .isInstanceOf(Map.class);
            Map<?, ?> map = (Map<?, ?>) ymlMap;
            if (map.containsKey(LockServiceStateLogger.OUTSTANDING_LOCK_REQUESTS_TITLE)) {
                Object arrayObj = map.get(LockServiceStateLogger.OUTSTANDING_LOCK_REQUESTS_TITLE);
                assertThat(arrayObj)
                        .describedAs("Outstanding lock requests is not a list")
                        .isInstanceOf(List.class);

                assertThat(((List<?>) arrayObj)).hasSameSizeAs(getOutstandingDescriptors());
            } else if (map.containsKey(LockServiceStateLogger.HELD_LOCKS_TITLE)) {
                Object mapObj = map.get(LockServiceStateLogger.HELD_LOCKS_TITLE);
                assertThat(mapObj).describedAs("Held locks is not a map").isInstanceOf(Map.class);

                assertThat(((Map<?, ?>) mapObj)).hasSameSizeAs(getHeldDescriptors());
            } else {
                throw new IllegalStateException("Map found in YAML document without an expected key");
            }
        }
    }

    @Test
    public void testSyncState() throws Exception {
        List<File> files = LockServiceTestUtils.logStateDirFiles();

        Optional<File> syncStateFile = files.stream()
                .filter(file -> file.getName().startsWith(LockServiceStateLogger.SYNC_STATE_FILE_PREFIX))
                .findFirst();

        assertThat(syncStateFile).isPresent();
        assertSyncStateStructureCorrect(syncStateFile.get());
        assertDescriptorsNotPresentInFile(syncStateFile.get());
    }

    @Test
    public void testSynthesizedRequestState() throws Exception {
        List<File> files = LockServiceTestUtils.logStateDirFiles();

        Optional<File> synthesizedRequestStateFile = files.stream()
                .filter(file -> file.getName().startsWith(LockServiceStateLogger.SYNTHESIZED_REQUEST_STATE_FILE_PREFIX))
                .findFirst();

        assertThat(synthesizedRequestStateFile).isPresent();
        assertSynthesizedRequestStateStructureCorrect(synthesizedRequestStateFile.get());
        assertDescriptorsNotPresentInFile(synthesizedRequestStateFile.get());
    }

    private void assertDescriptorsNotPresentInFile(File logFile) {
        try {
            List<String> contents = Files.readAllLines(logFile.toPath());
            String result = Strings.join(contents).with("\n");
            assertThat(result).doesNotContain(DESCRIPTOR_1.getLockIdAsString());
            assertThat(result).doesNotContain(DESCRIPTOR_2.getLockIdAsString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertSyncStateStructureCorrect(File syncStateFile) throws FileNotFoundException {
        Iterable<Object> syncState = new Yaml().loadAll(new FileInputStream(syncStateFile));

        for (Object ymlMap : syncState) {
            assertThat(ymlMap)
                    .describedAs("Sync state contains unrecognizable object")
                    .isInstanceOf(Map.class);
            Map<?, ?> map = (Map<?, ?>) ymlMap;
            if (map.containsKey(LockServiceStateLogger.SYNC_STATE_TITLE)) {
                Object mapObj = map.get(LockServiceStateLogger.SYNC_STATE_TITLE);
                assertThat(mapObj).describedAs("Sync state is not a map").isInstanceOf(Map.class);

                assertThat(((Map<?, ?>) mapObj)).hasSameSizeAs(getSyncStateDescriptors());
            } else {
                throw new IllegalStateException("Map found in YAML document without an expected key");
            }
        }
    }

    private void assertSynthesizedRequestStateStructureCorrect(File file) throws FileNotFoundException {
        Iterable<Object> synthesizedRequestState = new Yaml().loadAll(new FileInputStream(file));

        for (Object ymlMap : synthesizedRequestState) {
            assertThat(ymlMap)
                    .describedAs("Request state contains unrecognizable object")
                    .isInstanceOf(Map.class);
            Map<?, ?> map = (Map<?, ?>) ymlMap;
            if (map.containsKey(LockServiceStateLogger.SYNTHESIZED_REQUEST_STATE_TITLE)) {
                Object mapObj = map.get(LockServiceStateLogger.SYNTHESIZED_REQUEST_STATE_TITLE);
                assertThat(mapObj).describedAs("Request state is not a map").isInstanceOf(Map.class);

                assertThat(((Map<?, ?>) mapObj)).hasSameSizeAs(getSyncStateDescriptors());
            } else {
                throw new IllegalStateException("Map found in YAML document without an expected key");
            }
        }
    }

    @AfterClass
    public static void afterClass() throws IOException {
        LockServiceTestUtils.cleanUpLogStateDir();
    }

    private Set<LockDescriptor> getAllDescriptors() {
        Set<LockDescriptor> allDescriptors = new HashSet<>();

        allDescriptors.addAll(getOutstandingDescriptors());
        allDescriptors.addAll(getHeldDescriptors());

        return allDescriptors;
    }

    private Set<LockDescriptor> getHeldDescriptors() {
        return heldLocksTokenMap.values().stream()
                .map(LockServiceImpl.HeldLocks::getRealToken)
                .map(HeldLocksToken::getLockDescriptors)
                .flatMap(SortedLockCollection::stream)
                .collect(Collectors.toSet());
    }

    private Set<LockDescriptor> getOutstandingDescriptors() {
        return outstandingLockRequestMultimap.values().stream()
                .map(LockRequest::getLockDescriptors)
                .flatMap(SortedLockCollection::stream)
                .collect(Collectors.toSet());
    }

    private Set<LockDescriptor> getSyncStateDescriptors() {
        return syncStateMap.keySet();
    }
}
