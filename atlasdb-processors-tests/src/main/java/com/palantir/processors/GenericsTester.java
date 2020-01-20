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

package com.palantir.processors;

import java.util.List;
import java.util.Set;

@AutoDelegate
public interface GenericsTester<A, B extends List<C>, C> {
    A processItems(A first, B second, C third);

    List<B> getListOfLists(A comparison);

    @DoDelegate
    default Set<C> hello() {
        throw new UnsupportedOperationException("This class doesn't know how to say hello.");
    }

    static <A, D> D hidingParameter(A argument) {
        return null;
    }
}
