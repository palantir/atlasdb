/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.http;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public interface JepsenLockClient<TOKEN> {
    TOKEN lock(String client, String lockName) throws InterruptedException;

    Set<TOKEN> unlock(Set<TOKEN> tokens) throws InterruptedException;

    Set<TOKEN> refresh(Set<TOKEN> tokens) throws InterruptedException;

    default boolean unlockSingle(TOKEN token) throws InterruptedException {
        return token != null && !unlock(ImmutableSet.of(token)).isEmpty();
    }

    default TOKEN refreshSingle(TOKEN token) throws InterruptedException {
        return token == null ? null : Iterables.getOnlyElement(refresh(ImmutableSet.of(token)), null);
    }
}
