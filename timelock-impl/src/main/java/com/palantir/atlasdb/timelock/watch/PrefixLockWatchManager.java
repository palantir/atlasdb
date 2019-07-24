/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.watch;

import java.util.Set;

import com.palantir.atlasdb.timelock.watch.trie.PrefixTrie;
import com.palantir.lock.LockDescriptor;

public class PrefixLockWatchManager implements LockWatchManager {
    private final PrefixTrie<LockWatch> trie;

    public PrefixLockWatchManager(PrefixTrie<LockWatch> trie) {
        this.trie = trie;
    }

    @Override
    public LockEventProcessor getEventProcessor() {
        return new LockEventProcessor() {
            @Override
            public void registerLock(LockDescriptor descriptor) {
                Set<LockWatch> dataInTrieWithKeysPrefixesOf = trie.findDataInTrieWithKeysPrefixesOf(
                        descriptor.getLockIdAsString());
                dataInTrieWithKeysPrefixesOf.forEach(LockWatch::registerLock);
            }

            @Override
            public void registerUnlock(LockDescriptor descriptor) {
                trie.findDataInTrieWithKeysPrefixesOf(descriptor.getLockIdAsString())
                        .forEach(LockWatch::registerUnlock);
            }
        };
    }

    @Override
    public void seedProcessor(LockPredicate predicate, LockWatch watch) {
        if (predicate instanceof PrefixLockPredicate) {
            PrefixLockPredicate prefixLockPredicate = (PrefixLockPredicate) predicate;
            trie.add(prefixLockPredicate.prefix().prefix(), watch);
        }
    }

    @Override
    public void unseedProcessor(LockPredicate predicate, LockWatch watch) {
        // TODO (jkong): Implement prefix delete. I'm lazy and it's hackweek.
    }
}
