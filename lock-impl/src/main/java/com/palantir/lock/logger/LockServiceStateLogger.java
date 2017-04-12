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

import java.util.Collections;
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

public class LockServiceStateLogger {

    private static Logger log = LoggerFactory.getLogger(LockServiceStateLogger.class);

    private static final String OUTSTANDING_LOCK_REQUESTS_TITLE = "OutstandingLockRequests";
    private static final String HELD_LOCKS_TITLE = "HeldLocks";

    private String heldLocksYaml;
    private String outstandingLocksYaml;

    public LockServiceStateLogger(ConcurrentMap<HeldLocksToken, LockServiceImpl.HeldLocks<HeldLocksToken>> heldLocksTokenMap,
            SetMultimap<LockClient, LockRequest> outstandingLockRequestMultimap) {
        this.heldLocksYaml = generateHeldLocksYaml(heldLocksTokenMap);
        this.outstandingLocksYaml = generateOutstandingLocksYaml(Multimaps.asMap(outstandingLockRequestMultimap));
    }

    private String generateOutstandingLocksYaml(Map<LockClient, Set<LockRequest>> outstandingLockRequestsMap) {

        List<Map<String, SimpleLockRequest>> outstandingLockList = Collections.synchronizedList(Lists.newArrayList());

        outstandingLockRequestsMap.forEach((client, requestSet) -> {
            if (requestSet != null) {
                ImmutableSet<LockRequest> lockRequests = ImmutableSet.copyOf(requestSet);
                lockRequests.forEach(lockRequest ->
                        outstandingLockList.add(getDescriptorSimpleRequestMap(client, lockRequest)));
            }
        });

        Yaml yaml = new Yaml(getRepresenter(), getDumperOptions());
        return yaml.dump(nameObjectForYamlConvertion(outstandingLockList, OUTSTANDING_LOCK_REQUESTS_TITLE));
    }

    private String generateHeldLocksYaml(ConcurrentMap<HeldLocksToken, LockServiceImpl.HeldLocks<HeldLocksToken>> heldLocksTokenMap) {

        List<Map<String, Object>> mappedLockListGroupedByToken = heldLocksTokenMap.entrySet().stream()
                .map(heldLocksTokenMapEntry ->
                        getDescriptorToTokenMap(heldLocksTokenMapEntry.getKey(), heldLocksTokenMapEntry.getValue()))
                .collect(Collectors.toList());

        Map<String, Object> mappedLocksToToken = Maps.newConcurrentMap();
        mappedLockListGroupedByToken.forEach(mappedLocksToToken::putAll);

        Yaml yaml = new Yaml(getRepresenter(), getDumperOptions());
        return yaml.dump(nameObjectForYamlConvertion(mappedLocksToToken, HELD_LOCKS_TITLE));
    }

    private Map<String, Object> nameObjectForYamlConvertion(Object objectToName, String name) {
        return ImmutableMap.of(name, objectToName);
    }

    private Map<String, SimpleLockRequest> getDescriptorSimpleRequestMap(LockClient client, LockRequest request) {
        Map<String, SimpleLockRequest> requestsInfoMap = Maps.newHashMap();
        request.getLocks().forEach(lock -> {
            SimpleLockRequest simpleLockRequest = SimpleLockRequest.of(request, client.getClientId());
            requestsInfoMap.put(lock.getLockDescriptor().getLockIdAsString(), simpleLockRequest);
        });

        return requestsInfoMap;
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
        representer.addClassTag(SimpleTokenInfo.class, Tag.MAP);
        representer.addClassTag(SimpleLockRequest.class, Tag.MAP);
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

    public void logHeldLocks() {
        log.info(heldLocksYaml);
    }

    public void logOutstandingLockRequests() {
        log.info(outstandingLocksYaml);
    }
}