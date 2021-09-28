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

import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import com.palantir.common.annotation.LongTermSerialized;
import java.io.Serializable;
import java.util.Comparator;
import javax.annotation.concurrent.Immutable;

/**
 * A generic class for handling pairs of things.
 * @serial exclude
 * @author cfreeland
 * In general, it is good practice to make a usefully named class that holds the two
 *             things you care about. Typically, you'll find soon after you use this class that you
 *             really need to put 3 things in this "pair" anyways.
 */
@Immutable
@LongTermSerialized
public final class Pair<V, W> implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * The left-hand side, or first, object
     * in the <code>Pair</code>.
     * @see #getLhSide()
     */
    public final V lhSide;

    /**
     * The right-hand side, or second, object
     * in the <code>Pair</code>.
     */
    public final W rhSide;

    /**
     * Creates a new instance of <code>Pair</code>
     * with the left and right side set to the provided objects.
     * @param lh the object that will be the <code>lhSide</code> object.
     * @param rh the object that will be the <code>rhSide</code> object.
     */
    public Pair(V lh, W rh) {
        lhSide = lh;
        rhSide = rh;
    }

    /**
     * Creates a new instance of <code>Pair</code>
     * with both sides of the pair set to null.
     */
    public Pair() {
        this(null, null);
    }

    /**
     * Returns the left-hand side, or first, object
     * of the pair.
     */
    public V getLhSide() {
        return lhSide;
    }

    /**
     * Returns the right-hand side, or second, object
     * of the pair.
     */
    public W getRhSide() {
        return rhSide;
    }

    /**
     * Returns a pair with the left and right reversed.
     */
    public Pair<W, V> getReversed() {
        return create(rhSide, lhSide);
    }

    /**
     * Returns a <code>String</code> representation of
     * both items in the pair, left side first.
     */
    @Override
    public String toString() {
        StringBuilder out = new StringBuilder("Pair(");
        out.append(lhSide).append(", ").append(rhSide).append(")");
        return out.toString();
    }

    /**
     * Returns a hash code of the <code>Pair</code>'s values.
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((lhSide == null) ? 0 : lhSide.hashCode());
        result = prime * result + ((rhSide == null) ? 0 : rhSide.hashCode());
        return result;
    }

    /**
     * Returns <code>true</code> if this <code>Pair</code>
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
        if (getClass() != obj.getClass()) {
            return false;
        }
        Pair<?, ?> other = (Pair<?, ?>) obj;
        if (lhSide == null) {
            if (other.lhSide != null) {
                return false;
            }
        } else if (!lhSide.equals(other.lhSide)) {
            return false;
        }
        if (rhSide == null) {
            if (other.rhSide != null) {
                return false;
            }
        } else if (!rhSide.equals(other.rhSide)) {
            return false;
        }
        return true;
    }

    /**
     * Returns a new <code>Pair</code> instance
     * with the given left-side and right-side objects.
     */
    public static <V, W> Pair<V, W> create(V v, W w) {
        return new Pair<V, W>(v, w);
    }

    /**
     * Return a new <code>Pair</code> with the left hand side set to the passed value.
     */
    public <T> Pair<T, W> withLhSide(T newLhSide) {
        return Pair.create(newLhSide, rhSide);
    }

    /**
     * Returns a new <code>Pair</code> with the right hand side set to the passed value.
     */
    public <T> Pair<V, T> withRhSide(T newRhSide) {
        return Pair.create(lhSide, newRhSide);
    }

    /**
     * Returns a <code>Comparator</code> for the
     * left-hand side object of the <code>Pair</code>.
     */
    public static <T extends Comparable<? super T>, V> Ordering<Pair<T, V>> compareLhSide() {
        return new Ordering<Pair<T, V>>() {
            @Override
            public int compare(Pair<T, V> o1, Pair<T, V> o2) {
                return o1.lhSide.compareTo(o2.lhSide);
            }
        };
    }

    /**
     * Returns a <code>Comparator</code> for the
     * right-hand side object of the <code>Pair</code>.
     */
    public static <T, V extends Comparable<? super V>> Ordering<Pair<T, V>> compareRhSide() {
        return new Ordering<Pair<T, V>>() {
            @Override
            public int compare(Pair<T, V> o1, Pair<T, V> o2) {
                return o1.rhSide.compareTo(o2.rhSide);
            }
        };
    }

    /**
     * Returns a <code>Comparator</code> for the
     * left-hand side object of the <code>Pair</code>.
     */
    public static <T, W> Ordering<Pair<T, W>> compareLhSide(final Comparator<? super T> comp) {
        return new Ordering<Pair<T, W>>() {
            @Override
            public int compare(Pair<T, W> o1, Pair<T, W> o2) {
                return comp.compare(o1.lhSide, o2.lhSide);
            }
        };
    }

    /**
     * Returns a <code>Comparator</code> for the
     * right-hand side object of the <code>Pair</code>.
     */
    public static <T, W> Ordering<Pair<T, W>> compareRhSide(final Comparator<? super W> comp) {
        return new Ordering<Pair<T, W>>() {
            @Override
            public int compare(Pair<T, W> o1, Pair<T, W> o2) {
                return comp.compare(o1.rhSide, o2.rhSide);
            }
        };
    }

    public static <L, R> Function<Pair<L, R>, L> getLeftFunc() {
        return from -> from.lhSide;
    }

    public static <L, R> Function<Pair<L, R>, R> getRightFunc() {
        return from -> from.rhSide;
    }
}
