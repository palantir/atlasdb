/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.persist.Persistable;
import com.palantir.leader.NotCurrentLeaderException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SeedableDelegatingPaxosStateLog<V extends Persistable & Versionable> implements PaxosStateLog<V> {
    private static final Logger log = LoggerFactory.getLogger(SeedableDelegatingPaxosStateLog.class);

    private static final NotCurrentLeaderException NOT_CURRENT_LEADER_EXCEPTION = new NotCurrentLeaderException(
            "This node's Paxos state log is not ready yet, probably because we're migrating the Paxos store. Please "
                    + "wait...");

    private final AtomicReference<PaxosStateLog<V>> delegateReference = new AtomicReference<>();

    public void supplyDelegate(PaxosStateLog<V> delegate) {
        if (!delegateReference.compareAndSet(null, delegate)) {
            log.warn("Attempted to set the delegate of a seedable Paxos state log multiple times! This is indicative "
                    + "of a bug in the atlasdb or timelock code. We'll just use the first delegate.",
                    new RuntimeException("I exist to show you the stack trace"));
        }
    }

    @Override
    public void writeRound(long seq, V round) {
        executeOnDelegate(delegate -> delegate.writeRound(seq, round));
    }

    @Override
    public byte[] readRound(long seq) throws IOException {
        return checkedCallOnDelegate(delegate -> delegate.readRound(seq));
    }

    @Override
    public long getLeastLogEntry() {
        return callOnDelegate(PaxosStateLog::getLeastLogEntry);
    }

    @Override
    public long getGreatestLogEntry() {
        return callOnDelegate(PaxosStateLog::getGreatestLogEntry);
    }

    @Override
    public void truncate(long toDeleteInclusive) {
        executeOnDelegate(delegate -> delegate.truncate(toDeleteInclusive));
    }

    @Override
    public void truncateAllRounds() {
        executeOnDelegate(PaxosStateLog::truncateAllRounds);
    }

    private void executeOnDelegate(Consumer<PaxosStateLog<V>> consumer) {
        PaxosStateLog<V> delegate = delegateReference.get();
        if (delegate != null) {
            consumer.accept(delegate);
        } else {
            throw NOT_CURRENT_LEADER_EXCEPTION;
        }
    }

    private <T> T callOnDelegate(Function<PaxosStateLog<V>, T> function) {
        PaxosStateLog<V> delegate = delegateReference.get();
        if (delegate != null) {
            return function.apply(delegate);
        } else {
            throw NOT_CURRENT_LEADER_EXCEPTION;
        }
    }

    private <T, K extends Exception> T checkedCallOnDelegate(FunctionCheckedException<PaxosStateLog<V>, T, K> functionCheckedException) throws K {
        PaxosStateLog<V> delegate = delegateReference.get();
        if (delegate != null) {
            return functionCheckedException.apply(delegate);
        } else {
            throw NOT_CURRENT_LEADER_EXCEPTION;
        }
    }
}
