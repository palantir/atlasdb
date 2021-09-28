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

import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;

/**
 * Registers an mbean useful for tracking cache operations. Subclasses are largely responsible for
 * updating the mbean on puts, cache hits, cache misses, etc.
 */
public abstract class MBeanCache<K, V> implements NonDistributedCache<K, V> {

    protected final CacheStats mbean = new CacheStats(this);

    private volatile String name = "MBeanCache";

    /**
     * Sets the name for this cache.  Useful
     * for debugging.
     */
    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public abstract int getMaxCacheSize();

    public abstract void setMaxCacheSize(int size);

    public static final String OBJECT_NAME_PREFIX = "com.palantir.caching:type=";

    public String getStatBeanName() {
        return OBJECT_NAME_PREFIX + name;
    }

    /**
     * Registers an mbean for this cache with the provided object name.
     *
     * @param objectName
     *            the object name for the mbean
     */
    public void registerMBean(String objectName) {
        if (objectName == null) {
            throw new SafeIllegalArgumentException("objectName must not be null.");
        }

        JMXUtils.registerMBeanCatchAndLogExceptions(mbean, objectName);
    }

    /**
     * Make sure to get a unique name on this object before trying to register this cache with JMX.
     * If it doens't have a unique name, then it won't be registered correctly.
     */
    public void registerMBean() {
        registerMBean(getStatBeanName());
    }

    public void collectStatsLoadTimeForMiss(long loadTimeInMillis) {
        mbean.loadTimeForMisses.addAndGet(loadTimeInMillis);
    }

    public void collectStatsLoadTimeForCacheKey(long loadTimeInMillis) {
        mbean.loadTimeForCacheKey.addAndGet(loadTimeInMillis);
    }

    @Override
    public abstract int size();

    /** Clears all entries from the cache.
     */
    @Override
    public abstract void clear();
}
