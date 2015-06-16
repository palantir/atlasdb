// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.common.collect;

import java.util.Collection;
import java.util.Map;
import java.util.Set;


/**
 * Common super-interface for maps and multimaps.
 */
public interface MapCollector<K, V> {
    void put(K key, V value);
    boolean containsKey(K key);
    boolean isEmpty();
    int size();
    void remove(K key);
    void clear();
    Set<K> keySet();
    Collection<V> values();
    Collection<Map.Entry<K, V>> entrySet();
    boolean containsEntry(K key, V value);
}
