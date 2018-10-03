/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;

public final class LockStateDebugger {

    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory())
            .registerModule(new GuavaModule())
            .registerModule(new Jdk8Module());

    public static void main(String[] args) throws IOException {
        String filePath = "file_path";
        File file = new File(filePath);
        YamlFileFormat parsedFile = OBJECT_MAPPER.readValue(file, YamlFileFormat.class);

        /*
        // Build the graph of who's blocking who
        Map<String, Multimap<String, String>> blockers = Maps.newHashMap();
        for (OutstandingLockRequestsForDescriptor outstanding : parsedFile.getOutstandingLockRequests()) {
            String lockDescriptor = outstanding.getLockDescriptor();
            SimpleTokenInfo info = parsedFile.getHeldLocks().get(lockDescriptor);
            if (info != null) {
                String lockHoldingThread = info.getRequestThread();
                if (!blockers.containsKey(lockHoldingThread)) {
                    blockers.put(lockHoldingThread, ArrayListMultimap.create());
                }
                Multimap<String, String> blockingThreads = blockers.get(lockHoldingThread);
                for (SimpleLockRequest request : outstanding.getLockRequests()) {
                    String blockedThread = request.getCreatingThread();
                    blockingThreads.put(blockedThread, lockDescriptor);
                }
            } else {
                System.out.println("Unblocked thread(s)");
            }
        }
        */

        Map<String, Multimap<String, String>> blockedThreads = Maps.newHashMap();
        for (OutstandingLockRequestsForDescriptor outstanding : parsedFile.getOutstandingLockRequests()) {
            String lockDescriptor = outstanding.getLockDescriptor();
            String blockingThread = "NO THREAD";
            SimpleTokenInfo info = parsedFile.getHeldLocks().get(lockDescriptor);
            if (info != null) {
                blockingThread = info.getRequestThread();
            }

            for (SimpleLockRequest request : outstanding.getLockRequests()) {
                String blockedThread = request.getCreatingThread();
                if (!blockedThreads.containsKey(blockedThread)) {
                    blockedThreads.put(blockedThread, ArrayListMultimap.create());
                }
                Multimap<String, String> blockingThreads = blockedThreads.get(blockedThread);
                blockingThreads.put(blockingThread, lockDescriptor);
            }
        }

        // Now know which threads are holding locks directly blocking each thread
        // According to the support issue, expect some threads to be blocked on locks that no one is holding
        // Presumably the earliest requester for this lock is waiting to access it?
        // What if the earliest requester is currently blocked waiting for an earlier (sorted) lock? Does their
        //  claim on that lock still apply, or does it get ordered differently?

        // Follow-up question: for any thread blocked by NO THREAD, is there an earlier request that might lay
        // claim to that thread? Could this explain the behavior we're seeing?

        // We can order threads that are holding locks, but can't order any others aside from claims

        List<String> sortedThreads = parsedFile.getHeldLocks().values().stream()
                .distinct()
                .sorted((locka, lockb) -> Longs.compare(locka.getCreatedAtTs(), lockb.getCreatedAtTs()))
                .map(SimpleTokenInfo::getRequestThread)
                .collect(Collectors.toList());

        System.out.println("\n\n\nUnordered threads:");

        Set<String> unorderedThreads = Sets.newHashSet(blockedThreads.keySet());
        unorderedThreads.removeAll(sortedThreads);
        for (String blockedThread : unorderedThreads) {
            Multimap<String, String> blockingThreads = blockedThreads.get(blockedThread);
            System.out.println("Blocked thread: " + parseRequestThread(blockedThread));
            for (String blockingThreadId : blockingThreads.keySet()) {
                System.out.println("\tBlocked by: " + parseRequestThread(blockingThreadId) + " for locks: ");
                System.out.println("\t\t" + blockingThreads.get(blockingThreadId));
            }
            System.out.println();
        }

        System.out.println("\n\n\nTime ordered threads:");
        for (String blockedThread : sortedThreads) {
            Multimap<String, String> blockingThreads = blockedThreads.get(blockedThread);
            if (blockingThreads != null) {
                System.out.println("Blocked thread: " + parseRequestThread(blockedThread));
                for (String blockingThreadId : blockingThreads.keySet()) {
                    System.out.println("\tBlocked by: " + parseRequestThread(blockingThreadId) + " for locks: ");
                    System.out.println("\t\t" + blockingThreads.get(blockingThreadId));
                }
                System.out.println();
            }
        }

        // Figure out number of locks held by each thread
        Multimap<String, String> locksHeld = ArrayListMultimap.create();
        for (Map.Entry<String, SimpleTokenInfo> entry : parsedFile.getHeldLocks().entrySet()) {
            locksHeld.put(entry.getValue().getRequestThread(), entry.getKey());
        }

        System.out.println("\n\n\nUnblocked blocked threads:");
        // For threads that aren't blocked on anyone, are they all exhibiting the bug or is there an ordering between them?
        for (String blockedThread : blockedThreads.keySet()) {
            Multimap<String, String> blockingThreads = blockedThreads.get(blockedThread);
            if ((blockingThreads != null) && (blockingThreads.keySet().size() == 1) && (Iterables.getOnlyElement(blockingThreads.keySet()).equals("NO THREAD"))) {
                // This thread is blocked by no one, check to see what contention looks like for those locks
                System.out.println("Thread " + parseRequestThread(blockedThread));
                Set<String> lockDescriptorsNeeded = Sets.newHashSet(blockingThreads.values());
                for (OutstandingLockRequestsForDescriptor outstanding : parsedFile.getOutstandingLockRequests()) {
                    if (lockDescriptorsNeeded.contains(outstanding.getLockDescriptor())) {
                        // This lock is needed by this blocked thread
                        if ((outstanding.getLockRequests().size() == 1)
                                && (Iterables.getOnlyElement(outstanding.getLockRequests()).getCreatingThread().equals(blockedThread))) {
                            // Do nothing, we're the only ones requesting this lock
                        } else {
                            System.out.println("\tLock descriptor: " + outstanding.getLockDescriptor());
                            for (SimpleLockRequest request : outstanding.getLockRequests()) {
                                System.out.println("\t\t" + parseRequestThread(request.getCreatingThread())
                                        + " (which holds " + locksHeld.get(request.getCreatingThread()).size() + " locks)");
                            }
                        }
                    }
                }
                System.out.println();
            }
        }

        System.out.println("\n\n\nHeld locks:");
        for (String thread : locksHeld.keySet()) {
            System.out.println(parseRequestThread(thread) + " holds " + locksHeld.get(thread) + "\n");
        }

    }

    private static String parseRequestThread(String requestThread) {
        if (requestThread.equals("")) {
            return "NO THREAD";
        }
        String[] parts = requestThread.split(" ");
        return parts[0] + " " + parts[1];
    }

    @Value.Immutable
    abstract static class Blocker {
        public abstract List<String> blockedLockDescriptors();
        public abstract String threadId();

        public static Blocker of(List<String> blockedLockDescriptors, String threadId) {
            return ImmutableBlocker.builder().build();
        }
    }

    @JsonDeserialize(as = ImmutableYamlFileFormat.class)
    @JsonSerialize(as = ImmutableYamlFileFormat.class)
    @Value.Immutable
    abstract static class YamlFileFormat {

        public abstract List<OutstandingLockRequestsForDescriptor> getOutstandingLockRequests();

        public abstract Map<String, SimpleTokenInfo> getHeldLocks();

        public static YamlFileFormat of(List<OutstandingLockRequestsForDescriptor> outstanding,
                Map<String, SimpleTokenInfo> held) {
            return ImmutableYamlFileFormat.builder()
                    .addAllOutstandingLockRequests(outstanding)
                    .heldLocks(held)
                    .build();
        }
    }

    @JsonDeserialize(as = ImmutableOutstandingLockRequestsForDescriptor.class)
    @JsonSerialize(as = ImmutableOutstandingLockRequestsForDescriptor.class)
    @Value.Immutable
    abstract static class OutstandingLockRequestsForDescriptor {
        public abstract String getLockDescriptor();
        public abstract List<SimpleLockRequest> getLockRequests();
        public abstract int getLockRequestsCount();
    }
}
