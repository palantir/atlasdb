package com.palantir.atlasdb.keyvalue.partition;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;

import com.google.common.base.Preconditions;

public class CycleMap<K, V> implements NavigableMap<K, V>{

    private final NavigableMap<K, V> backingMap;

    private CycleMap(NavigableMap backingMap) {
        this.backingMap = backingMap;
    }

    public static <K, V> CycleMap<K, V> wrap(NavigableMap<K, V> backingMap) {
        return new CycleMap<>(backingMap);
    }

    public K nextKey(K key) {
        Preconditions.checkState(size() > 0);
        K next = higherKey(key);
        if (next == null) {
            next = firstKey();
        }
        return next;
    }

    public K previousKey(K key) {
        Preconditions.checkState(size() > 0);
        K previous = lowerKey(key);
        if (previous == null) {
            previous = lastKey();
        }
        return previous;
    }

    @Override
    public Comparator<? super K> comparator() {
        return backingMap.comparator();
    }

    @Override
    public K firstKey() {
        return backingMap.firstKey();
    }

    @Override
    public K lastKey() {
        return backingMap.lastKey();
    }

    @Override
    public Set<K> keySet() {
        return backingMap.keySet();
    }

    @Override
    public Collection<V> values() {
        return backingMap.values();
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return backingMap.entrySet();
    }

    @Override
    public int size() {
        return backingMap.size();
    }

    @Override
    public boolean isEmpty() {
        return backingMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return backingMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return backingMap.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return backingMap.get(key);
    }

    @Override
    public V put(K key, V value) {
        return backingMap.put(key, value);
    }

    @Override
    public V remove(Object key) {
        return backingMap.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        backingMap.putAll(m);
    }

    @Override
    public void clear() {
        backingMap.clear();
    }

    @Override
    public java.util.Map.Entry<K, V> lowerEntry(K key) {
        return backingMap.lowerEntry(key);
    }

    @Override
    public K lowerKey(K key) {
        return backingMap.lowerKey(key);
    }

    @Override
    public java.util.Map.Entry<K, V> floorEntry(K key) {
        return backingMap.floorEntry(key);
    }

    @Override
    public K floorKey(K key) {
        return backingMap.floorKey(key);
    }

    @Override
    public java.util.Map.Entry<K, V> ceilingEntry(K key) {
        return backingMap.ceilingEntry(key);
    }

    @Override
    public K ceilingKey(K key) {
        return backingMap.ceilingKey(key);
    }

    @Override
    public java.util.Map.Entry<K, V> higherEntry(K key) {
        return backingMap.higherEntry(key);
    }

    @Override
    public K higherKey(K key) {
        return backingMap.higherKey(key);
    }

    @Override
    public java.util.Map.Entry<K, V> firstEntry() {
        return backingMap.firstEntry();
    }

    @Override
    public java.util.Map.Entry<K, V> lastEntry() {
        return backingMap.lastEntry();
    }

    @Override
    public java.util.Map.Entry<K, V> pollFirstEntry() {
        return backingMap.pollFirstEntry();
    }

    @Override
    public java.util.Map.Entry<K, V> pollLastEntry() {
        return backingMap.pollLastEntry();
    }

    @Override
    public CycleMap<K, V> descendingMap() {
        return CycleMap.wrap(backingMap.descendingMap());
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
        return backingMap.navigableKeySet();
    }

    @Override
    public NavigableSet<K> descendingKeySet() {
        return backingMap.descendingKeySet();
    }

    @Override
    public NavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return backingMap.subMap(fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public NavigableMap<K, V> headMap(K toKey, boolean inclusive) {
        return backingMap.headMap(toKey, inclusive);
    }

    @Override
    public NavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
        return backingMap.tailMap(fromKey, inclusive);
    }

    @Override
    public SortedMap<K, V> subMap(K fromKey, K toKey) {
        return backingMap.subMap(fromKey, toKey);
    }

    @Override
    public SortedMap<K, V> headMap(K toKey) {
        return backingMap.headMap(toKey);
    }

    @Override
    public SortedMap<K, V> tailMap(K fromKey) {
        return backingMap.tailMap(fromKey);
    }

}
