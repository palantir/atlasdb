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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockRequest;
import com.palantir.lock.impl.ClientAwareReadWriteLock;
import com.palantir.lock.impl.LockServerLock;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.lock.impl.LockServiceStateDebugger;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

public class LockServiceStateLogger {
    private static Logger log = LoggerFactory.getLogger(LockServiceStateLogger.class);

    @VisibleForTesting
    static final String LOCKSTATE_FILE_PREFIX = "lockstate-";
    @VisibleForTesting
    static final String DESCRIPTORS_FILE_PREFIX = "descriptors-";
    @VisibleForTesting
    static final String SYNC_STATE_FILE_PREFIX = "sync-state-";
    @VisibleForTesting
    static final String SYNTHESIZED_REQUEST_STATE_FILE_PREFIX = "synthesized-requests-";

    @VisibleForTesting
    static final String OUTSTANDING_LOCK_REQUESTS_TITLE = "OutstandingLockRequests";
    @VisibleForTesting
    static final String HELD_LOCKS_TITLE = "HeldLocks";
    @VisibleForTesting
    static final String SYNC_STATE_TITLE = "SyncState";
    @VisibleForTesting
    static final String SYNTHESIZED_REQUEST_STATE_TITLE = "SynthesizedLockRequestState";

    private static final String WARNING_LOCK_DESCRIPTORS = "WARNING: Lock descriptors may contain sensitive information";
    private static final String FILE_NOT_CREATED_LOG_ERROR = "Destination file [{}] either already exists"
            + "or can't be created. This is a very unlikely scenario. "
            + "Retrigger logging or check if process has permissions on the folder";
    private static final String WARNING_SYNTHESIZE_ANONYMOUS = "WARNING: For LockClient.ANONYMOUS this method"
            + " may not return accurate results.";

    private final LockDescriptorMapper lockDescriptorMapper = new LockDescriptorMapper();
    private final long startTimestamp = System.currentTimeMillis();

    private final ConcurrentMap<HeldLocksToken, LockServiceImpl.HeldLocks<HeldLocksToken>> heldLocks;
    private final Map<LockClient, Set<LockRequest>> outstandingLockRequests;
    private final Map<LockDescriptor, ClientAwareReadWriteLock> descriptorToLockMap;
    private String outputDir;


    public LockServiceStateLogger(
            ConcurrentMap<HeldLocksToken, LockServiceImpl.HeldLocks<HeldLocksToken>> heldLocksTokenMap,
            SetMultimap<LockClient, LockRequest> outstandingLockRequestMultimap,
            Map<LockDescriptor, ClientAwareReadWriteLock> descriptorToLockMap,
            String outputDir) {
        this.heldLocks = heldLocksTokenMap;
        this.outstandingLockRequests = Multimaps.asMap(outstandingLockRequestMultimap);
        this.descriptorToLockMap = descriptorToLockMap;
        this.outputDir = outputDir;
    }

    public void logLocks() throws IOException {
        Map<String, Object> generatedOutstandingRequests = generateOutstandingLocksYaml(outstandingLockRequests);
        Map<String, Object> generatedHeldLocks = generateHeldLocks(heldLocks);
        Map<String, Object> generatedSyncState = generateSyncState(descriptorToLockMap);
        Map<String, Object> synthesizedRequestState
                = synthesizeRequestState(outstandingLockRequests, descriptorToLockMap);

        Path outputDirPath = Paths.get(outputDir);
        Files.createDirectories(outputDirPath);

        dumpYamlsInNewFiles(
                generatedOutstandingRequests, generatedHeldLocks, generatedSyncState, synthesizedRequestState);
    }

    private Map<String, Object> synthesizeRequestState(
            Map<LockClient, Set<LockRequest>> outstandingLockRequests,
            Map<LockDescriptor, ClientAwareReadWriteLock> descriptorToLockMap) {
        LockServiceStateDebugger debugger = new LockServiceStateDebugger(outstandingLockRequests, descriptorToLockMap);
        Multimap<LockClient, LockServiceStateDebugger.LockRequestProgress> progressMultimap
                = debugger.getSuspectedLockProgress();

        return nameObjectForYamlConversion(SYNTHESIZED_REQUEST_STATE_TITLE, progressMultimap.asMap().entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> entry.getKey().toString(),
                        entry -> entry.getValue().stream().map(
                                lockRequestProgress -> SanitizedLockRequestProgress.create(
                                        lockRequestProgress, lockDescriptorMapper, entry.getKey().toString()))
                                .collect(Collectors.toList()))));
    }

    private Map<String, Object> generateOutstandingLocksYaml(Map<LockClient, Set<LockRequest>> outstandingLockRequestsMap) {
        Map<String, SimpleLockRequestsWithSameDescriptor> outstandingRequestMap = Maps.newHashMap();

        outstandingLockRequestsMap.forEach((client, requestSet) -> {
            if (requestSet != null) {
                ImmutableSet<LockRequest> lockRequests = ImmutableSet.copyOf(requestSet);
                lockRequests.forEach(lockRequest -> {
                    List<SimpleLockRequest> requestList = getDescriptorSimpleRequestMap(client, lockRequest);
                    requestList.forEach(request -> {
                        outstandingRequestMap.putIfAbsent(request.getLockDescriptor(),
                                new SimpleLockRequestsWithSameDescriptor(request.getLockDescriptor()));
                        outstandingRequestMap.get(request.getLockDescriptor()).addLockRequest(request);
                    });
                });
            }
        });

        List<SimpleLockRequestsWithSameDescriptor> sortedOutstandingRequests = sortOutstandingRequests(outstandingRequestMap.values());

        return nameObjectForYamlConversion(OUTSTANDING_LOCK_REQUESTS_TITLE, sortedOutstandingRequests);
    }

    private List<SimpleLockRequestsWithSameDescriptor> sortOutstandingRequests(
            Collection<SimpleLockRequestsWithSameDescriptor> outstandingRequestsByDescriptor) {

        List<SimpleLockRequestsWithSameDescriptor> sortedEntries = Lists.newArrayList(outstandingRequestsByDescriptor);
        sortedEntries.sort(
                (o1, o2) -> Integer.compare(o2.getLockRequestsCount(),
                        o1.getLockRequestsCount()));
        return sortedEntries;
    }

    private Map<String, Object> generateHeldLocks(ConcurrentMap<HeldLocksToken, LockServiceImpl.HeldLocks<HeldLocksToken>> heldLocksTokenMap) {
        Map<String, Object> mappedLocksToToken = Maps.newHashMap();
        heldLocksTokenMap.values().forEach(locks -> mappedLocksToToken.putAll(getDescriptorToTokenMap(locks.getRealToken())));

        return nameObjectForYamlConversion(HELD_LOCKS_TITLE, mappedLocksToToken);
    }

    private Map<String, Object> generateSyncState(Map<LockDescriptor, ClientAwareReadWriteLock> descriptorToLockMap) {
        return nameObjectForYamlConversion(SYNC_STATE_TITLE,
                descriptorToLockMap.entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                entry -> lockDescriptorMapper.getDescriptorMapping(entry.getKey()),
                                entry -> ((LockServerLock) entry.getValue()).toSanitizedString())));
    }

    private Map<String, Object> nameObjectForYamlConversion(String name, Object objectToName) {
        return ImmutableMap.of(name, objectToName);
    }

    private List<SimpleLockRequest> getDescriptorSimpleRequestMap(LockClient client, LockRequest request) {
        return request.getLocks().stream()
                .map(lock ->
                        SimpleLockRequest.of(request,
                                this.lockDescriptorMapper.getDescriptorMapping(lock.getLockDescriptor()),
                                lock.getLockMode(),
                                client.getClientId()))
                .collect(Collectors.toList());
    }

    private Map<String, Object> getDescriptorToTokenMap(HeldLocksToken realToken) {
        Map<String, Object> lockToLockInfo = Maps.newHashMap();

        realToken.getLocks().forEach(
                lock -> lockToLockInfo.put(
                            this.lockDescriptorMapper.getDescriptorMapping(lock.getLockDescriptor()),
                            SimpleTokenInfo.of(realToken, lock.getLockMode()))
        );
        return lockToLockInfo;
    }

    private void dumpYamlsInNewFiles(
            Map<String, Object> generatedOutstandingRequests,
            Map<String, Object> generatedHeldLocks,
            Map<String, Object> generatedSyncState,
            Map<String, Object> synthesizedRequestState) throws IOException {
        File lockStateFile = createNewFile(LOCKSTATE_FILE_PREFIX);
        File descriptorsFile = createNewFile(DESCRIPTORS_FILE_PREFIX);
        File syncStateFile = createNewFile(SYNC_STATE_FILE_PREFIX);
        File synthesizedRequestStateFile = createNewFile(SYNTHESIZED_REQUEST_STATE_FILE_PREFIX);

        dumpYaml(ImmutableList.of(generatedOutstandingRequests, generatedHeldLocks), lockStateFile);
        dumpYaml(ImmutableList.of(generatedSyncState), syncStateFile);
        dumpYaml(ImmutableList.of(synthesizedRequestState), synthesizedRequestStateFile, WARNING_SYNTHESIZE_ANONYMOUS);
        dumpDescriptorsYaml(descriptorsFile);
    }

    private File createNewFile(String fileNamePrefix) throws IOException {
        String fileName = fileNamePrefix + this.startTimestamp + ".yaml";
        File file = new File(outputDir, fileName);

        if (!file.createNewFile()) {
            log.error(FILE_NOT_CREATED_LOG_ERROR, file.getAbsolutePath());
            throw new IllegalStateException(
                    MessageFormatter.format(FILE_NOT_CREATED_LOG_ERROR, file.getAbsolutePath()).getMessage());
        }

        return file;
    }

    private void dumpYaml(List<Map<String, Object>> objects, File file) throws IOException {
        try (LockStateYamlWriter writer = LockStateYamlWriter.create(file)) {
            for (Map<String, Object> object : objects) {
                writer.dumpObject(object);
            }
        }
    }

    private void dumpYaml(List<Map<String, Object>> objects, File file, String prefix) throws IOException {
        try (LockStateYamlWriter writer = LockStateYamlWriter.create(file)) {
            writer.appendComment(prefix);
            for (Map<String, Object> object : objects) {
                writer.dumpObject(object);
            }
        }
    }

    private void dumpDescriptorsYaml(File descriptorsFile) throws IOException {
        try (LockStateYamlWriter writer = LockStateYamlWriter.create(descriptorsFile)) {
            writer.appendComment(WARNING_LOCK_DESCRIPTORS);
            writer.dumpObject(lockDescriptorMapper.getReversedMapper());
        }
    }
}
