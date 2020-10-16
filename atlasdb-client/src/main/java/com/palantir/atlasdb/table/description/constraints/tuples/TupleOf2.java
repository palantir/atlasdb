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
package com.palantir.atlasdb.table.description.constraints.tuples;

public class TupleOf2<A, B> implements Tuple {
    private final A one;
    private final B two;

    public static <A, B> TupleOf2<A, B> of(A a, B b) {
        return new TupleOf2<A, B>(a, b);
    }

    public TupleOf2(A a, B b) {
        this.one = a;
        this.two = b;
    }

    public A field1() {
        return one;
    }

    public B field2() {
        return two;
    }

    @Override
    public int getSize() {
        return 2;
    }
}
