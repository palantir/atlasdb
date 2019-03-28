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

package com.palantir.atlasdb.timelock.lock;

import com.palantir.lock.v2.Lease;

public class Leased<T> {
    private final T value;
    private final Lease lease;

    private Leased (T value, Lease lease) {
        this.value = value;
        this.lease = lease;
    }

    public static <T> Leased<T> of(T value, Lease lease) {
        return new Leased<>(value, lease);
    }

    public T value() {
        return value;
    }

    public Lease lease() {
        return lease;
    }
}
