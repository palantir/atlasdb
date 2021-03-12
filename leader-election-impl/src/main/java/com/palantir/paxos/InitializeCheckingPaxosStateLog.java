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

import com.palantir.common.persist.Persistable;
import com.palantir.leader.NotCurrentLeaderException;
import java.io.IOException;
import java.util.function.Supplier;

public class InitializeCheckingPaxosStateLog<V extends Persistable & Versionable> implements PaxosStateLog<V> {
    private static final NotCurrentLeaderException NOT_CURRENT_LEADER_EXCEPTION = new NotCurrentLeaderException(
            "This node is not ready to serve requests yet! Please wait, or try the other leader nodes.");

    private final PaxosStateLog<V> delegate;

    public InitializeCheckingPaxosStateLog(PaxosStateLog<V> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void writeRound(long seq, V round) {
        runWithInitializeCheck(() -> {
            delegate.writeRound(seq, round);
            return null;
        });
    }

    @Override
    public byte[] readRound(long seq) throws IOException {
        // Written slightly differently to allow for checked exceptions
        if (delegate.isInitialized()) {
            return delegate.readRound(seq);
        }
        throw NOT_CURRENT_LEADER_EXCEPTION;
    }

    @Override
    public long getLeastLogEntry() {
        return runWithInitializeCheck(delegate::getLeastLogEntry);
    }

    @Override
    public long getGreatestLogEntry() {
        return runWithInitializeCheck(delegate::getGreatestLogEntry);
    }

    @Override
    public void truncate(long toDeleteInclusive) {
        runWithInitializeCheck(() -> {
            delegate.truncate(toDeleteInclusive);
            return null;
        });
    }

    @Override
    public void truncateAllRounds() {
        runWithInitializeCheck(() -> {
            delegate.truncateAllRounds();
            return null;
        });
    }

    @Override
    public boolean isInitialized() {
        // This shouldn't throw even if we aren't initialized
        return delegate.isInitialized();
    }

    private <T> T runWithInitializeCheck(Supplier<T> operation) {
        if (delegate.isInitialized()) {
            return operation.get();
        }
        throw NOT_CURRENT_LEADER_EXCEPTION;
    }
}
