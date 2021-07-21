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
package com.palantir.util;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

/**
 * Keyed Weak Reference.
 *
 */
public class KeyedWeakReference<K, V> extends WeakReference<V> implements KeyedReference {

    private final K key;

    public KeyedWeakReference(K key, V value) {
        super(value);
        this.key = key;
    }

    public KeyedWeakReference(K key, V value, ReferenceQueue<V> queue) {
        super(value, queue);
        this.key = key;
    }

    @Override
    public K getKey() {
        return key;
    }
}
