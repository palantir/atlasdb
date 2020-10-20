/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.paxos;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.persist.Persistable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class PaxosStateLogBatchReader<V extends Persistable & Versionable> implements AutoCloseable {
    private final PaxosStateLog<V> delegate;
    private final Persistable.Hydrator<V> hydrator;
    private final ListeningExecutorService executor;

    public PaxosStateLogBatchReader(PaxosStateLog<V> delegate, Persistable.Hydrator<V> hydrator, int numThreads) {
        this.delegate = delegate;
        this.hydrator = hydrator;
        this.executor = MoreExecutors.listeningDecorator(PTExecutors.newFixedThreadPool(numThreads, "psl-reader"));
    }

    /**
     * Reads entries from startSequence (inclusive) to startSequence + numEntries (exclusive) from the delegate log.
     *
     * @param startSequence first sequence to read
     * @param numEntries number of entries to read
     * @return a list of paxos rounds for all the present entries in the delegate log
     */
    public List<PaxosRound<V>> readBatch(long startSequence, int numEntries) {
        return AtlasFutures.getUnchecked(Futures.allAsList(LongStream.range(startSequence, startSequence + numEntries)
                        .mapToObj(sequence -> executor.submit(() -> singleRead(sequence)))
                        .collect(Collectors.toList())))
                .stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    private Optional<PaxosRound<V>> singleRead(long sequence) {
        try {
            return Optional.ofNullable(delegate.readRound(sequence))
                    .map(bytes -> PaxosRound.of(sequence, hydrator.hydrateFromBytes(bytes)));
        } catch (IOException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}
