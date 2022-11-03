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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;

public final class TrackingIteratorTestUtils {
    public static final String STRING = "test";
    // this has to be an anonymous inner class rather than lambda in order to spy
    public static final ToLongFunction<String> STRING_MEASURER = new ToLongFunction<>() {
        @Override
        public long applyAsLong(String value) {
            return value.length();
        }
    };
    private static final ImmutableList<String> STRINGS =
            ImmutableList.of("test1", "test2", "test200", "composite", "", "t", "tt");

    public static Iterator<String> createStringIterator() {
        return STRINGS.stream().iterator();
    }

    public static <T> List<T> consumeIteratorIntoList(Iterator<T> iterator) {
        ArrayList<T> list = new ArrayList<>();
        iterator.forEachRemaining(list::add);
        return list;
    }

    // this has to be a static method rather than a static member in order to be used for any type T
    public static <T> Consumer<T> noOp() {
        // this has to be an anonymous inner class rather than lambda in order to spy
        return new Consumer<>() {
            @Override
            public void accept(T t) {}
        };
    }
}
