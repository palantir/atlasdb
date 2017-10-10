/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import java.util.Set;

import com.google.common.collect.Sets;
import com.palantir.async.initializer.AsyncInitializer;

public class TransactionManagerState {
    private static TransactionManagerState instance;

    private Set<AsyncInitializer> initializables = Sets.newHashSet();

    private static void create() {
        if (instance == null) {
            instance = new TransactionManagerState();
        }
    }

    public static synchronized boolean isInitialized() {
        create();
        return instance.initializables.stream().allMatch(AsyncInitializer::isInitialized);
    }

    public static synchronized void register(AsyncInitializer initializer) {
        create();
        instance.initializables.add(initializer);
    }

}
