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
package com.palantir.atlasdb.keyvalue.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.UnmodifiableIterator;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IterablePartitioner {
    private static final Logger defaultLogger = LoggerFactory.getLogger(IterablePartitioner.class);

    private static final String ENTRY_TOO_BIG_MESSAGE = "Encountered an entry of approximate size {} bytes,"
            + " larger than maximum size of {} defined per entire batch,"
            + " while doing a write to {}. Attempting to batch anyways.";

    private IterablePartitioner() {
        // Utility class
    }

    public static <T> Iterable<List<T>> partitionByCountAndBytes(
            final Iterable<T> iterable,
            final int maximumCountPerPartition,
            final long maximumBytesPerPartition,
            final TableReference tableRef,
            final Function<T, Long> sizingFunction) {
        return partitionByCountAndBytes(
                iterable,
                maximumCountPerPartition,
                maximumBytesPerPartition,
                tableRef.getQualifiedName(),
                sizingFunction);
    }

    public static <T> Iterable<List<T>> partitionByCountAndBytes(
            final Iterable<T> iterable,
            final int maximumCountPerPartition,
            final long maximumBytesPerPartition,
            final String tableNameForLoggingPurposesOnly,
            final Function<T, Long> sizingFunction) {
        return partitionByCountAndBytes(
                iterable,
                maximumCountPerPartition,
                maximumBytesPerPartition,
                tableNameForLoggingPurposesOnly,
                sizingFunction,
                defaultLogger);
    }

    // FIXME: The tableNameForLoggingPurposesOnly is *not* always a valid tableName
    // This string should *not* be used or treated as a real tableName, even though sometimes it is.
    // For example, CassandraKVS multiPuts can cause this string to include *multiple* tableNames
    @VisibleForTesting
    public static <T> Iterable<List<T>> partitionByCountAndBytes(
            final Iterable<T> iterable,
            final int maximumCountPerPartition,
            final long maximumBytesPerPartition,
            final String tableNameForLoggingPurposesOnly,
            final Function<T, Long> sizingFunction,
            final Logger log) {
        return () -> new UnmodifiableIterator<List<T>>() {
            PeekingIterator<T> pi = Iterators.peekingIterator(iterable.iterator());
            private int remainingEntries = Iterables.size(iterable);

            @Override
            public boolean hasNext() {
                return pi.hasNext();
            }

            @Override
            public List<T> next() {
                if (!pi.hasNext()) {
                    throw new NoSuchElementException();
                }
                List<T> entries = new ArrayList<>(Math.min(maximumCountPerPartition, remainingEntries));
                long runningSize = 0;

                // limit on: maximum count, pending data, maximum size, but allow at least one even if it's too huge
                T firstEntry = pi.next();
                runningSize += sizingFunction.apply(firstEntry);
                entries.add(firstEntry);
                if (runningSize > maximumBytesPerPartition && log.isWarnEnabled()) {

                    if (AtlasDbConstants.TABLES_KNOWN_TO_BE_POORLY_DESIGNED.contains(
                            TableReference.createWithEmptyNamespace(tableNameForLoggingPurposesOnly))) {
                        log.warn(
                                ENTRY_TOO_BIG_MESSAGE,
                                sizingFunction.apply(firstEntry),
                                maximumBytesPerPartition,
                                tableNameForLoggingPurposesOnly);
                    } else {
                        final String longerMessage =
                                ENTRY_TOO_BIG_MESSAGE + " This can potentially cause out-of-memory errors.";
                        log.warn(
                                longerMessage,
                                SafeArg.of("approximatePutSize", sizingFunction.apply(firstEntry)),
                                SafeArg.of("maximumPutSize", maximumBytesPerPartition),
                                // FIXME: This must be an unsafe arg because it is not necessarily a real tableName
                                UnsafeArg.of("tableName", tableNameForLoggingPurposesOnly));
                    }
                }

                while (pi.hasNext() && entries.size() < maximumCountPerPartition) {
                    runningSize += sizingFunction.apply(pi.peek());
                    if (runningSize > maximumBytesPerPartition) {
                        break;
                    }
                    entries.add(pi.next());
                }
                remainingEntries -= entries.size();
                return entries;
            }
        };
    }
}
