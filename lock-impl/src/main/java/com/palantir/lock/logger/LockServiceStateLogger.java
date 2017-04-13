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
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

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
import com.palantir.lock.logger.security.StringEncoder;

public class LockServiceStateLogger {
    private static Logger log = LoggerFactory.getLogger(LockServiceStateLogger.class);

    private static final String OUTSTANDING_LOCK_REQUESTS_TITLE = "OutstandingLockRequests";
    private static final String HELD_LOCKS_TITLE = "HeldLocks";

    private static final String FILE_NOT_CREATED_LOG_ERROR = "Destination file [{}] either already exists"
            + "or can't be created. This is a very unlikely scenario."
            + "Retrigger logging or check if process has permitions on the folder";

    private final StringEncoder stringEncoder = new StringEncoder();

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

        long timestamp = System.currentTimeMillis();

        Path outputDirPath = Paths.get(outputDir);
        Files.createDirectories(outputDirPath);

        dumpYamlInNewFile(outputDir, generatedOutstandingRequests, generatedHeldLocks, timestamp);
        dumpKeyInNewFile(outputDir, timestamp);
    }

    private Map<String, Object> generateOutstandingLocksYaml(Map<LockClient, Set<LockRequest>> outstandingLockRequestsMap) {
        Map<String, SimpleLockRequestsWithSameDescriptor> outstandingRequestMap = Maps.newConcurrentMap();

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

        Yaml yaml = new Yaml(getRepresenter(), getDumperOptions());
        return nameObjectForYamlConvertion(sortedOutstandingRequests, OUTSTANDING_LOCK_REQUESTS_TITLE);
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

        List<Map<String, Object>> mappedLockListGroupedByToken = heldLocksTokenMap.entrySet().stream()
                .map(heldLocksTokenMapEntry ->
                        getDescriptorToTokenMap(heldLocksTokenMapEntry.getKey(), heldLocksTokenMapEntry.getValue()))
                .collect(Collectors.toList());

        Map<String, Object> mappedLocksToToken = Maps.newConcurrentMap();
        mappedLockListGroupedByToken.forEach(mappedLocksToToken::putAll);

        return nameObjectForYamlConvertion(mappedLocksToToken, HELD_LOCKS_TITLE);
    }

    private Map<String, Object> nameObjectForYamlConvertion(Object objectToName, String name) {
        return ImmutableMap.of(name, objectToName);
    }

    private List<SimpleLockRequest> getDescriptorSimpleRequestMap(LockClient client, LockRequest request) {
        return request.getLocks().stream()
                .map(lock ->
                        SimpleLockRequest.of(request,
                                this.stringEncoder.encrypt(lock.getLockDescriptor().getLockIdAsString()),
                                client.getClientId()))
                .collect(Collectors.toList());
    }

    private DumperOptions getDumperOptions() {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setIndent(4);
        options.setAllowReadOnlyProperties(true);
        return options;
    }

    private Representer getRepresenter() {
        Representer representer = new Representer();
        representer.addClassTag(ImmutableSimpleTokenInfo.class, Tag.MAP);
        representer.addClassTag(ImmutableSimpleLockRequest.class, Tag.MAP);
        representer.addClassTag(SimpleLockRequestsWithSameDescriptor.class, Tag.MAP);
        return representer;
    }

    private Map<String, Object> getDescriptorToTokenMap(HeldLocksToken heldLocksToken,
            LockServiceImpl.HeldLocks<HeldLocksToken> heldLocks) {
        Map<String, Object> lockToLockInfo = Maps.newHashMap();
        heldLocks.getLockDescriptors()
                .forEach(lockDescriptor ->
                        lockToLockInfo.put(lockDescriptor.getLockIdAsString(), SimpleTokenInfo.of(heldLocksToken)));
        return lockToLockInfo;
    }

    private void dumpYamlInNewFile(String outputDir, Map<String, Object> generatedOutstandingRequests,
            Map<String, Object> generatedHeldLocks, long timestamp) throws IOException {
        String fileName = "lockstate-" + timestamp + ".yaml";

        File file = new File(outputDir, fileName);

        if (file.createNewFile()) {
            dumpYaml(generatedOutstandingRequests, generatedHeldLocks, file);
        } else {
            log.error(FILE_NOT_CREATED_LOG_ERROR, file.getAbsolutePath());
            throw new IllegalStateException("Yaml file either already exists or can't be created at: " + file.getAbsolutePath());
        }
    }

    private void dumpYaml(Map<String, Object> generatedOutstandingRequests, Map<String, Object> generatedHeldLocks,
            File file) throws IOException {
        FileWriter writer = new FileWriter(file);
        Yaml yaml = new Yaml(getRepresenter(), getDumperOptions());
        yaml.dump(generatedOutstandingRequests, writer);
        writer.append("\n\n---\n\n");
        yaml.dump(generatedHeldLocks, writer);
    }

    private void dumpKeyInNewFile(String outputDir, long timestamp) throws IOException {
        String keyFileName = "key-" + timestamp + ".key";
        File keyFile = new File(outputDir, keyFileName);
        if (keyFile.createNewFile()) {
            Files.write(keyFile.toPath(), stringEncoder.getKey());
        } else {
            log.error(FILE_NOT_CREATED_LOG_ERROR, keyFile.getAbsolutePath());
            throw new IllegalStateException("Key file can't be created at: " + keyFile.getAbsolutePath());
        }
    }
}