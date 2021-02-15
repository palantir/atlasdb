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

/**
 * This interface exists because many cache calls don't make sense in a distributed sense.
 * For example, invalidating a cache element is not guaranteed to work,  if you depend on this
 * behavior for correctness, then it will be an error.  The only thing valid to store in a distributed
 * cache are immutable elements, because it is not guaranteed the cache will get updates as you propagate them.
 *
 * Caches may clear out elements at will, you may not rely on the cache containing an element.  You also may not
 * rely on the absence of an object.  Another server may put this object there.  You also may no rely on the newer
 * version being in the cache.
 *
 * This interface is here to be very strict with you so you don't "do the wrong thing" when it comes to
 * distributed caching.
 *
 * There are more complicated caching models with consistent caches where you push invalidates to all clients,
 * and they either all respond, or you have to wait the whole client timeout before you may do the write.
 *
 */
public interface DistributedCacheMgrCache<K, V> {
    V put(K key, V value);

    /**
     * The only thing this call ensures is that it will either return null
     * or some value that was {@link #put(Object, Object)} for this key.
     * If put is called many times, it may be any of these values.
     *
     * If v1 then v2 is put in this cache, this get call may return null, v1, null, v2, null, v1.
     */
    V get(K key);
}
