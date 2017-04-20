/**
 * Copyright 2017 Palantir Technologies
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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRequest;
import com.palantir.lock.impl.LockServiceImpl;

public class LockServiceStateLogger {
    private static Logger log = LoggerFactory.getLogger(LockServiceStateLogger.class);

    private static final String LOCKSTATE_FILE_PREFIX = "lockstate-";
    private static final String DESCRIPTORS_FILE_PREFIX = "descriptors-";
    private static final String WARNING_LOCK_DESCRIPTORS = "# WARNING: Lock descriptors may contain sensitive information\n";
    private static final String FILE_NOT_CREATED_LOG_ERROR = "Destination file [%s] either already exists"
            + "or can't be created. This is a very unlikely scenario."
            + "Retrigger logging or check if process has permitions on the folder";


    private static final String OUTSTANDING_LOCK_REQUESTS_TITLE = "OutstandingLockRequests";
    private static final String HELD_LOCKS_TITLE = "HeldLocks";

    private final LockDescriptorMapper lockDescriptorMapper = new LockDescriptorMapper();
    private final long startTimestamp = System.currentTimeMillis();

    private final ConcurrentMap<HeldLocksToken, LockServiceImpl.HeldLocks<HeldLocksToken>> heldLocks;
    private final Map<LockClient, Set<LockRequest>> outstandingLockRequests;


    public LockServiceStateLogger(ConcurrentMap<HeldLocksToken, LockServiceImpl.HeldLocks<HeldLocksToken>> heldLocksTokenMap,
            SetMultimap<LockClient, LockRequest> outstandingLockRequestMultimap) {
        this.heldLocks = heldLocksTokenMap;
        this.outstandingLockRequests = Multimaps.asMap(outstandingLockRequestMultimap);
    }

    public void logLocks(String outputDir) throws IOException {
        Map<String, Object> generatedOutstandingRequests = generateOutstandingLocksYaml(outstandingLockRequests);
        Map<String, Object> generatedHeldLocks = generateHeldLocks(heldLocks);

        Path outputDirPath = Paths.get(outputDir);
        Files.createDirectories(outputDirPath);

        dumpYamlsInNewFiles(outputDir, generatedOutstandingRequests, generatedHeldLocks);
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

        return nameObjectForYamlConvertion(OUTSTANDING_LOCK_REQUESTS_TITLE, sortedOutstandingRequests);
    }

    private List<SimpleLockRequestsWithSameDescriptor> sortOutstandingRequests(
            Collection<SimpleLockRequestsWithSameDescriptor> outstandingRequestsByDescriptor) {

        List<SimpleLockRequestsWithSameDescriptor> sortedEntries = Lists.newArrayList(outstandingRequestsByDescriptor);
        sortedEntries.sort(
                new Comparator<SimpleLockRequestsWithSameDescriptor>() {
                    @Override
                    public int compare(SimpleLockRequestsWithSameDescriptor o1,
                            SimpleLockRequestsWithSameDescriptor o2) {
                        return Integer.compare(o2.getLockRequestsCount(),
                                o1.getLockRequestsCount());
                    }
                });
        return sortedEntries;
    }

    private Map<String, Object> generateHeldLocks(ConcurrentMap<HeldLocksToken, LockServiceImpl.HeldLocks<HeldLocksToken>> heldLocksTokenMap) {
        Map<String, Object> mappedLocksToToken = Maps.newHashMap();
        heldLocksTokenMap.forEach((token, locks) -> {
            mappedLocksToToken.putAll(getDescriptorToTokenMap(token, locks));
        });

        return nameObjectForYamlConvertion(HELD_LOCKS_TITLE, mappedLocksToToken);
    }

    private Map<String, Object> nameObjectForYamlConvertion(String name, Object objectToName) {
        return ImmutableMap.of(name, objectToName);
    }

    private List<SimpleLockRequest> getDescriptorSimpleRequestMap(LockClient client, LockRequest request) {
        return request.getLocks().stream()
                .map(lock ->
                        SimpleLockRequest.of(request,
                                this.lockDescriptorMapper.getDescriptorMapping(lock.getLockDescriptor().getLockIdAsString()),
                                client.getClientId()))
                .collect(Collectors.toList());
    }

    private Map<String, Object> getDescriptorToTokenMap(HeldLocksToken heldLocksToken,
            LockServiceImpl.HeldLocks<HeldLocksToken> heldLocks) {
        Map<String, Object> lockToLockInfo = Maps.newHashMap();
        heldLocks.getLockDescriptors()
                .forEach(lockDescriptor ->
                        lockToLockInfo.put(
                                this.lockDescriptorMapper.getDescriptorMapping(lockDescriptor.getLockIdAsString()),
                                SimpleTokenInfo.of(heldLocksToken)));
        return lockToLockInfo;
    }

    private void dumpYamlsInNewFiles(String outputDir, Map<String, Object> generatedOutstandingRequests,
            Map<String, Object> generatedHeldLocks) throws IOException {
        String fileName = LOCKSTATE_FILE_PREFIX + this.startTimestamp + ".yaml";
        File file = new File(outputDir, fileName);

        String descriptorsFileName = DESCRIPTORS_FILE_PREFIX + this.startTimestamp + ".yaml";
        File descriptorsFile = new File(outputDir, descriptorsFileName);

        createNewFile(file);
        createNewFile(descriptorsFile);

        LockLoggingYamlOptions yamlOptions = new LockLoggingYamlOptions();

        dumpYaml(generatedOutstandingRequests, generatedHeldLocks, file, yamlOptions);
        dumpDescriptorsYaml(descriptorsFile, yamlOptions);
    }

    private void createNewFile(File file) throws IOException {
        if (!file.createNewFile()) {
            String fileCreationError = String.format(FILE_NOT_CREATED_LOG_ERROR, file.getAbsolutePath());
            log.error(fileCreationError);
            throw new IllegalStateException(fileCreationError);
        }
    }

    private void dumpYaml(Map<String, Object> generatedOutstandingRequests, Map<String, Object> generatedHeldLocks,
            File file, LockLoggingYamlOptions yamlOptions) throws IOException {

        FileWriter writer = new FileWriter(file);

        Yaml yaml = new Yaml(yamlOptions.getRepresenter(), yamlOptions.getDumperOptions());
        yaml.dump(generatedOutstandingRequests, writer);
        writer.append("\n\n---\n\n");
        yaml.dump(generatedHeldLocks, writer);
    }

    private void dumpDescriptorsYaml(File descriptorsFile, LockLoggingYamlOptions yamlOptions) throws IOException {
        FileWriter writer = new FileWriter(descriptorsFile);
        writer.append(WARNING_LOCK_DESCRIPTORS);
        Yaml yaml = new Yaml(yamlOptions.getRepresenter(), yamlOptions.getDumperOptions());
        yaml.dump(this.lockDescriptorMapper.getReversedMapper(), writer);
    }
}