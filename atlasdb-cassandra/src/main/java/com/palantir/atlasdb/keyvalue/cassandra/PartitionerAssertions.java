/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.collect.Streams;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PartitionerAssertions {
    private static final SafeLogger log = SafeLoggerFactory.get(PartitionerAssertions.class);
    private static final String PARTITIONING_ERROR_MESSAGE = "Some elements were changed during partitioning!";

    private PartitionerAssertions() {
        // Utility
    }

    public static <T> void checkPartitioning(
            Map<InetSocketAddress, List<T>> partitionedByHost, Iterable<T> originalElements) {
        Stream<T> partitionedElements = partitionedByHost.values().stream().flatMap(List::stream);
        Map<T, Long> partitionedElementCounts = countElements(partitionedElements);
        if (!partitionedElementCounts.equals(countElements(Streams.stream(originalElements)))) {
            log.error(
                    PARTITIONING_ERROR_MESSAGE,
                    UnsafeArg.of("elementsToBePartitioned", originalElements),
                    UnsafeArg.of("proposedPartition", partitionedElements));
            throw new SafeIllegalStateException(
                    PARTITIONING_ERROR_MESSAGE,
                    UnsafeArg.of("elementsToBePartitioned", originalElements),
                    UnsafeArg.of("proposedPartition", partitionedElements));
        }
    }

    private static <T> Map<T, Long> countElements(Stream<T> stream) {
        return stream.collect(Collectors.groupingBy(x -> x, Collectors.counting()));
    }
}
