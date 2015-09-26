/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.util;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.WeakHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Thread Safe
 *
 */
public class SoftCache<K, V> implements NonDistributedCache<K, V> {
    private static final int INITIAL_SIZE = 1000;

    private static final Logger log = LoggerFactory.getLogger(SoftCache.class);

    final protected Map<K, CacheEntry<V>> cacheEntries;

    final protected CacheStats mbean = new CacheStats(this);

    volatile private String name = "SoftCache";

    public SoftCache() {
        this(INITIAL_SIZE);
    }

    public SoftCache(int initialSize) {
        cacheEntries = createCache(initialSize);
        SoftCache.registerForCleanup(this);
    }

    /**
     * Sets the name for this cache.  Useful
     * for debugging.
     *
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public int getMaxCacheSize() {
        return -1;
    }

    public void setMaxCacheSize(int size) {
        /* do nothing here.  subclasses will override */
    }

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
            throw new IllegalArgumentException("objectName must not be null.");
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

    /**
     * This method should be over-ridden by subclasses to change
     * the underlying cache implementation and implement features
     * like LRU caches...
     *
     * @param initialSize
     * @return
     */
    protected Map<K, CacheEntry<V>> createCache(int initialSize) {
        return new HashMap<K, CacheEntry<V>>(initialSize);
    }

    protected final ReferenceQueue<V> referenceQueue = new ReferenceQueue<V>();

    /* Basic map operations
     *******************
     */

    @Deprecated
    public void  stopCleanupThread() {
//        This operation does nothing
//        Do not call it anymore
    }

    @Deprecated
    public void startCleanupThread() {
//        This operation does nothing
//        Do not call it anymore
    }

    private synchronized void removeReference(Reference<? extends V> ref){
        if (ref instanceof KeyedReference){
            Object key = ((KeyedReference) ref).getKey();
            CacheEntry<V> entry = cacheEntries.get(key);

            // only remove the cache entry if it holds the current reference
            // (it could have already been replaced by a new entry)
            if (entry != null && entry.valueRef == ref){
                if (log.isDebugEnabled()){
                    log.debug("Removing from cache reference with key: " + key);
                }

                cacheEntries.remove(key);
            }
        } else{
            assert false : "All references should be of type KeyedReference";
        }
    }

    @Override
    public synchronized boolean containsKey(K key) {
        V val = get(key);
        return (val != null);
    }

    public synchronized boolean containsValue(V val) {
        for (CacheEntry<V> entry : cacheEntries.values()) {
            V myValue = entry.getValue();
            if (myValue != null && myValue.equals(val)) {
                return true;
            }
        }
        return false;
    }

    /** Adds an object to the cache.
     * @param key
     * @param value
     */
    @Override
    public synchronized V put(K key, V value) {
        mbean.puts.incrementAndGet();
        CacheEntry<V> entry = newSoftCacheEntry(key, value);
        CacheEntry<V> oldEntry = cacheEntries.put(key, entry);

        return safeValue(oldEntry);
    }

    public void collectStatsLoadTimeForMiss(long loadTimeInMillis) {
        mbean.loadTimeForMisses.addAndGet(loadTimeInMillis);
    }

    public void collectStatsLoadTimeForCacheKey(long loadTimeInMillis) {
        mbean.loadTimeForCacheKey.addAndGet(loadTimeInMillis);
    }

    /**
     * If the specified key is not already associated with a value, associate it with the given value. This is equivalent to
     *
     * @param key
     * @param value
     * @return The value that was in the cache, null if none was there before
     */
    public synchronized V putIfAbsent(K key, V value)
    {
        if (!containsKey(key))
            return put(key, value);
        else
            return get(key);
    }

    public synchronized void putAll(Map<? extends K, ? extends V> map) {
        for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    public synchronized void putAllIfAbsent(Map<? extends K, ? extends V> map) {
        for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
            putIfAbsent(entry.getKey(), entry.getValue());
        }
    }

    public void putAllIfAbsent(Map<? extends K, ? extends V> map, long loadTimeInMillis) {
        mbean.loadTimeForMisses.addAndGet(loadTimeInMillis);
        putAllIfAbsent(map);
    }


    /** Gets an object from the cache.
     * @param key
     */
    @Override
    public synchronized V get(K key)
    {
        CacheEntry<V> entry = cacheEntries.get(key);

        // a) not cached, return null
        if(entry == null) {
            mbean.misses.incrementAndGet();
            if (log.isTraceEnabled()) {
                log.trace("Cache miss (not cached) on " + key);
            }
            return null;
        }

        //must get the hard ref before the check to isValid, otherwise it could become invalid before (c)
        V ret = entry.getValue();

        // b) stale entry, remove it
        if(!entry.isValid()) {
            mbean.misses.incrementAndGet();
            if (log.isTraceEnabled()) {
                log.trace("Cache miss (stale entry) on " + key);
            }
            cacheEntries.remove(key);
            return null;
        }

        // c) fresh and valid, return value
        if (log.isTraceEnabled()) {
            log.trace("Cache hit on " + key);
        }
        mbean.hits.incrementAndGet();
        return ret;
    }

    /** Removes an object from the cache.
     * @param key
     */
    public synchronized V remove(K key) {
        CacheEntry<V> entry = cacheEntries.remove(key);
        return safeValue(entry);
    }

    @Override
    public synchronized int size() {
        return cacheEntries.size();
    }

    /** Clears all entries from the cache.
     */
    @Override
    public synchronized void clear() {
        cacheEntries.clear();
    }

    private V safeValue(CacheEntry<V> entry) {
        return (entry != null) ? entry.getValue() : null;
    }

    public synchronized Set<K> keySet() {
        return ImmutableSet.copyOf(cacheEntries.keySet());
    }

    public synchronized Set<V> removeMatchingKeys(com.google.common.base.Predicate<K> predicate) {
        Set<V> removedValues = Sets.newHashSet();

        Iterator<Entry<K, CacheEntry<V>>> entryIterator = cacheEntries.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Entry<K, CacheEntry<V>> entry = entryIterator.next();
            if (predicate.apply(entry.getKey())) {
                entryIterator.remove();
                removedValues.add(entry.getValue().getValue());

            }
        }

        return removedValues;
    }

    /* Soft reference operations
     ***************************
     */

    /**
     * Iterates through the cache and cleans up any cache references that have
     * been collected by the garbage collector.
     */
    public final void cleanup() {
        mbean.cleanups.incrementAndGet();
        if(log.isTraceEnabled()) {
            log.trace("cleanup() called on " + name + " of size: " + cacheEntries.size());
        }

        int i = 0;
        Reference<? extends V> ref = referenceQueue.poll();
        while (ref != null) {
            i++;
            assert ref.get() == null :  "Referent should be null by the time the Reference is added to the queue";
            removeReference(ref);
            ref = referenceQueue.poll();
        }

        if(log.isTraceEnabled()) {
            log.trace("cleanup() finished on " + name + ".  " + i + " keys were cleaned up. ");
        }
    }

    /*
     * Convenience cache operations *****************************
     */

    /** This convenience method filters a request by removing all items in the request which
     * are in the cache and returning the corresponding values.
     *
     * Synchronization note: this method is not synchronized on the cache.  Thus, if replacements
     * are performed during a canonicalization, it is undefined which object is returned.  Similarly,
     * this function is not synchronized on the request collection, so if synchronization is required,
     * it must be performed externally.
     *
     * @param request The list of items to be fetched from the backing store.  This collection must
     * be modifiable.
     */
    public Collection<V> filter(Collection<K> request) {
        Collection<V> rv = new ArrayList<V>();

        for (Iterator<K> iter = request.iterator(); iter.hasNext();) {
            K key = iter.next();

            V val = get(key);

            if (val != null) {
                rv.add(val);
                iter.remove();
            }
        }

        return rv;
    }

    /** This convenience method takes a map of items returned from the backing store and replaces
     * references loaded from the backing store with items in the cache.
     *
     * A call to canonicalize will typically be followed by a putAll on the returnVal, so that
     * future requests to the cache will return the new items loaded.
     *
     * Synchronization note: this method is not synchronized on the cache.  Thus, if replacements
     * are performed during a canonicalization, it is undefined which object is returned.  Similarly,
     * this function is not synchronized on the returnVal map, so if synchronization is required, it
     * must be performed externally.
     *
     * @param returnVal The map of items to be canonicalized.  This map must be modifiable.
     */
    public void canonicalize(Map<K, V> returnVal) {
        Map<K, V> canonicalizedMappings = new HashMap<K, V>();
        for (Iterator<Map.Entry<K, V> > iter = returnVal.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<K, V> entry = iter.next();

            V myValue = get(entry.getKey());

            if (myValue != null) {
                iter.remove();
                canonicalizedMappings.put(entry.getKey(), myValue);
            }
        }

        returnVal.putAll(canonicalizedMappings);
    }


    /**
     * CacheEntry is a cache entry that stores its value as a soft reference.  Cache entries do not
     * time out, so it is extremely important that these entries be invalidated if the underlying
     * objects change.
     */
    protected static abstract class CacheEntry<V> {
        protected Reference<V> valueRef;

        protected CacheEntry(Reference<V> ref){
            this.valueRef = ref;
        }

        V getValue() {
            return valueRef.get();
        }

        public void clear() {
            valueRef.clear();
        }

        public boolean isValid() {
            return valueRef.get() != null;
        }

        @Override
        public String toString() {
            return (valueRef.get() != null)?valueRef.get().toString():"null";
        }
    }

    protected SoftCacheEntry<K, V> newSoftCacheEntry(K key, V value) {
        return new SoftCacheEntry<K, V>(key, value, referenceQueue);
    }

    protected static class SoftCacheEntry<K, V> extends CacheEntry<V> {
        private SoftCacheEntry(K key, V value, ReferenceQueue<V> queue) {
            super(new KeyedSoftReference<K, V>(key, value, queue));
        }
    }

    protected WeakCacheEntry<K, V> newWeakCacheEntry(K key, V value) {
        return new WeakCacheEntry<K, V>(key, value, referenceQueue);
    }

    protected static class WeakCacheEntry<K, V> extends CacheEntry<V> {
        private WeakCacheEntry(K key, V value, ReferenceQueue<V> queue) {
            super(new KeyedWeakReference<K, V>(key, value, queue));
        }
    }

    @Override
    public synchronized String toString() {
        return "SoftCache named " + name + ": " + cacheEntries.values().toString();
    }

    /**
     * Registering your cache will hold a weak ref to it and for as long as it is still referenced,
     * it will be cleaned up on a separate thread.  This eliminates the need of using {@link #startCleanupThread()}
     * and {@link #stopCleanupThread()}
     *
     * @deprecated this is deprecated, because it is done automatically from now on, so it will be made private soon
     */
    @Deprecated
    public synchronized static void registerForCleanup(SoftCache<?, ?> cache) {
        cachesForCleanup.put(cache, null);
    }

    private static final WeakHashMap<SoftCache<?, ?>, Void> cachesForCleanup = new WeakHashMap<SoftCache<?, ?>, Void>();

    private synchronized static Collection<SoftCache<?, ?>> getCachesForCleanup() {
        return new ArrayList<SoftCache<?, ?>>(cachesForCleanup.keySet());
    }

    static {
        startStaticCleanupThread();
    }

    private static final int CLEANUP_DELAY = 10 * 1000;

    private static void startStaticCleanupThread() {
        Timer t = new Timer("SoftCache Cleanup Thread", true);

        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                try {
                    for (SoftCache<?, ?> c : getCachesForCleanup()) {
                        // Weak maps can produce null if the key is no longer in memory.
                        if (c != null) {
                            c.cleanup();
                        }
                    }
                } catch (Throwable e) {
                    log.error("Cleanup task has failed.", e);
                }
            }
        };

        t.schedule(task, 0, CLEANUP_DELAY);
    }

}
