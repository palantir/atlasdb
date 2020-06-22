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

package com.palantir.atlasdb.keyvalue.api.watch;

import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.ImmutableTransactionsLockWatchUpdate;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import java.util.List;
import java.util.Map;
import org.immutables.value.Value;

@Value.Immutable
interface ClientLogEvents {

    List<LockWatchEvent> events();

    boolean clearCache();

    default TransactionsLockWatchUpdate map(Map<Long, IdentifiedVersion> timestampMap) {
        return ImmutableTransactionsLockWatchUpdate.builder()
                .startTsToSequence(timestampMap)
                .events(events())
                .clearCache(clearCache())
                .build();
    }

    class Builder extends ImmutableClientLogEvents.Builder {}
}
