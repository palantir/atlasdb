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

import java.io.Serializable;

/**
 * Simple generic class to use in situations where something needs to be final to be passed into a closure.
 * This should be preferred over using an array of size 1 to pass an object around.  The reference
 * is also volatile so it can be used for cross-thread purposes.
 *
 * @author carrino
 * @serial exclude
 *
 * @param <T> the type of the value being enclosed
 */
public class Mutable<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private volatile T value;

    public static <T> Mutable<T> create() {
        return new Mutable<T>();
    }

    /**
     * Constructs and returns an empty
     * <code>Mutable</code>. Use {@link #set(Object)}
     * to add the enclosed object.
     */
    public Mutable() {
        this(null);
    }

    /**
     * Constructs and returns a new <code>Mutable</code>
     * containing the given <code>val</code>.
     */
    public Mutable(T val) {
        value = val;
    }

    /**
     * Sets the enclosed object for an empty
     * <code>Mutable</code>.
     */
    public void set(T val) {
        value = val;
    }

    /**
     * Returns the value enclosed by this <code>Mutable</code>.
     */
    public T get() {
        return value;
    }

    /**
     * Returns a hash code of the <code>Mutable</code>'s value.
     */
    @Override
    public int hashCode() {
        T v = get();
        if (v == null) {
            return 0;
        }
        return v.hashCode();
    }

    /**
     * Returns <code>true</code> if this <code>Mutable</code>
     * is equal to the given object.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Mutable<?>)) {
            return false;
        }
        final Mutable<?> other = (Mutable<?>) obj;
        T v = get();
        Object otherV = other.get();

        if (v == null) {
            if (otherV != null) {
                return false;
            }
        } else if (!v.equals(otherV)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
