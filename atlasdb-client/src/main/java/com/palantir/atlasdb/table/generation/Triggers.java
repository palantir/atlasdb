/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.table.generation;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.transaction.api.Transaction;

public class Triggers {
    public static <T, U extends T> List<T> getAllTriggers(Transaction t,
                                             List<Function<? super Transaction, U>> sharedTriggers,
                                             T[] triggers) {

        List<T> allTriggers = Lists.newArrayListWithCapacity(sharedTriggers.size() + triggers.length);
        for (T trigger : triggers) {
            allTriggers.add(trigger);
        }
        for (Function<? super Transaction, ? extends T> sharedTrigger : sharedTriggers) {
            allTriggers.add(sharedTrigger.apply(t));
        }
        return allTriggers;
    }
}
