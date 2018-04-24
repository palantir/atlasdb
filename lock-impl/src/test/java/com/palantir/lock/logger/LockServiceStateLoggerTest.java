/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.lock.logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockCollections;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.SortedLockCollection;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.impl.LockServiceImpl;

public class LockServiceStateLoggerTest {

    private static final ConcurrentMap<HeldLocksToken, LockServiceImpl.HeldLocks<HeldLocksToken>> heldLocksTokenMap =
            new MapMaker().makeMap();

    private static final SetMultimap<LockClient, LockRequest> outstandingLockRequestMultimap =
            Multimaps.synchronizedSetMultimap(HashMultimap.<LockClient, LockRequest>create());

    @BeforeClass
    public static void setUp() throws Exception {
        LockServiceTestUtils.cleanUpLogStateDir();

        LockClient clientA = LockClient.of("Client A");
        LockClient clientB = LockClient.of("Client B");

        LockDescriptor descriptor1 = StringLockDescriptor.of("logger-lock");
        LockDescriptor descriptor2 = StringLockDescriptor.of("logger-AAA");

        LockRequest request1 = LockRequest.builder(
                LockCollections.of(ImmutableSortedMap.of(descriptor1, LockMode.WRITE)))
                .blockForAtMost(SimpleTimeDuration.of(1000, TimeUnit.MILLISECONDS))
                .build();
        LockRequest request2 = LockRequest.builder(
                LockCollections.of(ImmutableSortedMap.of(descriptor2, LockMode.WRITE)))
                .blockForAtMost(SimpleTimeDuration.of(1000, TimeUnit.MILLISECONDS))
                .build();

        outstandingLockRequestMultimap.put(clientA, request1);
        outstandingLockRequestMultimap.put(clientB, request2);
        outstandingLockRequestMultimap.put(clientA, request2);

        HeldLocksToken token = LockServiceTestUtils.getFakeHeldLocksToken("client A", "Fake thread",
                new BigInteger("1"), "held-lock-1",
                "logger-lock");
        HeldLocksToken token2 = LockServiceTestUtils.getFakeHeldLocksToken("client B", "Fake thread 2",
                new BigInteger("2"), "held-lock-2",
                "held-lock-3");

        heldLocksTokenMap.putIfAbsent(token, LockServiceImpl.HeldLocks.of(token, LockCollections.of()));
        heldLocksTokenMap.putIfAbsent(token2, LockServiceImpl.HeldLocks.of(token2, LockCollections.of()));

        LockServiceStateLogger logger = new LockServiceStateLogger(
                heldLocksTokenMap,
                outstandingLockRequestMultimap,
                LockServiceTestUtils.TEST_LOG_STATE_DIR);
        logger.logLocks();
    }

    @Test
    public void testFilesExist() throws Exception {
        List<File> files = LockServiceTestUtils.logStateDirFiles();

        assertEquals("Unexpected number of descriptor files", files.stream()
                .filter(file -> file.getName().startsWith(LockServiceStateLogger.DESCRIPTORS_FILE_PREFIX))
                .count(), 1);

        assertEquals("Unexpected number of lock state files", files.stream()
                .filter(file -> file.getName().startsWith(LockServiceStateLogger.LOCKSTATE_FILE_PREFIX))
                .count(), 1);
    }

    @Test
    public void testDescriptors() throws Exception {
        List<File> files = LockServiceTestUtils.logStateDirFiles();

        Optional<File> descriptorsFile = files.stream().filter(
                file -> file.getName().startsWith(LockServiceStateLogger.DESCRIPTORS_FILE_PREFIX)).findFirst();

        Map<String, String> descriptorsMap = new Yaml().loadAs(new FileInputStream(descriptorsFile.get()), Map.class);

        Set<LockDescriptor> allDescriptors = getAllDescriptors();

        for (LockDescriptor descriptor : allDescriptors) {
            assertTrue("Existing descriptor can't be found in dumped descriptors",
                    descriptorsMap.values().stream().anyMatch(descriptorFromFile -> descriptorFromFile.equals(descriptor.toString())));
        }
    }

    @Test
    public void testLockState() throws Exception {
        List<File> files = LockServiceTestUtils.logStateDirFiles();

        Optional<File> lockStateFile = files.stream().filter(
                file -> file.getName().startsWith(LockServiceStateLogger.LOCKSTATE_FILE_PREFIX)).findFirst();

        Iterable<Object> lockState = new Yaml().loadAll(new FileInputStream(lockStateFile.get()));

        for (Object ymlMap : lockState) {
            assertTrue("Lock state contains unrecognizable object", ymlMap instanceof Map);
            Map map = (Map) ymlMap;
            if (map.containsKey(LockServiceStateLogger.OUTSTANDING_LOCK_REQUESTS_TITLE)) {
                Object arrayObj = map.get(LockServiceStateLogger.OUTSTANDING_LOCK_REQUESTS_TITLE);
                assertTrue("Outstanding lock requests is not a list", arrayObj instanceof List);

                assertEquals(getOutstandingDescriptors().size(), ((List) arrayObj).size());
            } else if (map.containsKey(LockServiceStateLogger.HELD_LOCKS_TITLE)) {
                Object mapObj = map.get(LockServiceStateLogger.HELD_LOCKS_TITLE);
                assertTrue("Held locks is not a list", mapObj instanceof Map);

                assertEquals(getHeldDescriptors().size(), ((Map) mapObj).size());
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
        Set<LockDescriptor> allDescriptors = Sets.newHashSet();

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
}
