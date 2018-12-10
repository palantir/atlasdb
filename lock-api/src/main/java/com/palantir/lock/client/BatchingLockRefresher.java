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

package com.palantir.lock.client;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.common.base.Throwables;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;

public final class BatchingLockRefresher {
    private final DisruptorAutobatcher<Set<LockToken>, Set<LockToken>> autobatcher;

    private BatchingLockRefresher(DisruptorAutobatcher<Set<LockToken>, Set<LockToken>> autobatcher) {
        this.autobatcher = autobatcher;
    }

    public static BatchingLockRefresher create(TimelockService timelockService) {
        return new BatchingLockRefresher(DisruptorAutobatcher.create(batch -> {
            Set<LockToken> tokensBatch = batch.stream()
                    .map(BatchElement::argument)
                    .flatMap(Set::stream)
                    .collect(Collectors.toSet());

            Set<LockToken> refreshedTokens = timelockService.refreshLockLeases(tokensBatch);
            batch.forEach(element -> element.result().set(
                    element.argument().stream()
                            .filter(refreshedTokens::contains)
                            .collect(Collectors.toSet())));
        }));
    }

    public Set<LockToken> refreshLockLeases(Set<LockToken> lockTokens) {
        try {
            return autobatcher.apply(lockTokens).get();
        } catch (ExecutionException e) {
            throw Throwables.throwUncheckedException(e.getCause());
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }
}
