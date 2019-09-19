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


import com.google.common.collect.Maps;
import com.palantir.lock.LockDescriptor;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


/**
 * Lock descriptors may contain obfuscated sensitive information. We want to make it impossible to get a real descriptor,
 * from the dump, even using brute force. We enumerate existing lock descriptors inside this class to use just "Lock-ID"
 * instead the real one. Mapping to original descriptors should be dumped in a separate file
 * with an explicit warning on the first line.
 */
class LockDescriptorMapper {

    private static final String LOCK_PREFIX = "Lock-";

    private Map<LockDescriptor, String> mapper = Maps.newConcurrentMap();
    private AtomicInteger lockCounter = new AtomicInteger();

    String getDescriptorMapping(LockDescriptor descriptor) {
        return mapper.computeIfAbsent(descriptor, k -> LOCK_PREFIX + lockCounter.incrementAndGet());
    }

    /**
     *
     * @return map from mapping to real descriptor.
     */
    Map<String, LockDescriptor> getReversedMapper() {
        return mapper.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }
}
