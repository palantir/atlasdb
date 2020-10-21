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
package com.palantir.lock;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Replacement for when you would normally use Map&lt;T, LockMode&gt;,
 * but don't actually need the keys to be a set, or to have random
 * lookup of values.
 */
public class LockCollection<T> extends AbstractCollection<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final T[] keys;
    protected final BitSet values;

    @SuppressWarnings("unchecked")
    LockCollection(Collection<Map.Entry<T, LockMode>> locks) {
        this.keys = (T[]) new Object[locks.size()];
        this.values = new BitSet(locks.size());
        int index = 0;
        for (Map.Entry<T, LockMode> entry : locks) {
            keys[index] = entry.getKey();
            if (entry.getValue() == LockMode.WRITE) {
                values.set(index);
            }
            index++;
        }
    }

    @Override
    public int size() {
        return keys.length;
    }

    public List<T> getKeys() {
        return Collections.unmodifiableList(Arrays.asList(this.keys));
    }

    public boolean hasReadLock() {
        return values.nextClearBit(0) < keys.length;
    }

    public Iterable<Map.Entry<T, LockMode>> entries() {
        return () -> new AbstractIterator<Map.Entry<T, LockMode>>() {
            private int index = 0;

            @Override
            protected Map.Entry<T, LockMode> computeNext() {
                if (index == keys.length) {
                    return endOfData();
                }
                LockMode mode = values.get(index) ? LockMode.WRITE : LockMode.READ;
                return Maps.immutableEntry(keys[index++], mode);
            }
        };
    }

    @Override
    public Iterator<T> iterator() {
        return getKeys().iterator();
    }

    @Override
    public String toString() {
        return "LockCollection " + Iterables.toString(entries());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(keys);
        result = prime * result + ((values == null) ? 0 : values.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        @SuppressWarnings("rawtypes")
        LockCollection other = (LockCollection) obj;
        if (!Arrays.equals(keys, other.keys)) {
            return false;
        }
        if (values == null) {
            if (other.values != null) {
                return false;
            }
        } else if (!values.equals(other.values)) {
            return false;
        }
        return true;
    }
}
