package com.palantir.atlasdb.keyvalue.partition;

import java.util.NavigableMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.ForwardingNavigableMap;

public class CycleMap<K, V> extends ForwardingNavigableMap<K, V>{
    private final NavigableMap<K, V> backingMap;

    private CycleMap(NavigableMap<K, V> backingMap) {
        this.backingMap = backingMap;
    }

    @Override
    protected NavigableMap<K, V> delegate() {
        return backingMap;
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
    public CycleMap<K, V> descendingMap() {
        return CycleMap.wrap(backingMap.descendingMap());
    }

}
