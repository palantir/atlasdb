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
package com.palantir.lock.impl;

import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.palantir.lock.LockClient;
import com.palantir.logsafe.Preconditions;

@ThreadSafe
@VisibleForTesting
public class LockClientIndices {
    private final Map<LockClient, Integer> indexByClient = Maps.newConcurrentMap();
    private final Map<Integer, LockClient> clientByIndex = Maps.newConcurrentMap();

    public LockClientIndices() {
        indexByClient.put(LockClient.ANONYMOUS, -1);
        clientByIndex.put(-1, LockClient.ANONYMOUS);
    }

    int toIndex(LockClient client) {
        Integer index = indexByClient.get(client);
        if (index != null) {
            return index;
        }
        synchronized (this) {
            index = indexByClient.get(client);
            if (index != null) {
                return index;
            }
            int newIndex = indexByClient.size();
            indexByClient.put(client, newIndex);
            clientByIndex.put(newIndex, client);
            return newIndex;
        }
    }

    LockClient fromIndex(int index) {
        return Preconditions.checkNotNull(clientByIndex.get(index));
    }

    Iterable<LockClient> fromIndices(Iterable<Integer> indices) {
        return Iterables.transform(indices, index -> fromIndex(index));
    }
}
