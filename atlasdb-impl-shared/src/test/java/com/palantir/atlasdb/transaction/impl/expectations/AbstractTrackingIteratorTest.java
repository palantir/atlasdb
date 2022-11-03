/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.expectations;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;

public class AbstractTrackingIteratorTest {
    protected static final String STRING = "test";
    protected static final ImmutableList<String> STRINGS =
            ImmutableList.of("test1", "test2", "test200", "composite", "", "t", "tt");
    protected static final Function<String, Long> STRING_MEASURER = Functions.compose(Long::valueOf, String::length);

    protected static Iterator<String> createStringIterator() {
        return STRINGS.stream().iterator();
    }

    protected static <T> Consumer<T> noOp() {
        // avoiding lambdas for mocking and using a method instead of a static object for generic use
        return new Consumer<>() {
            @Override
            public void accept(T t) {}
        };
    }
}
