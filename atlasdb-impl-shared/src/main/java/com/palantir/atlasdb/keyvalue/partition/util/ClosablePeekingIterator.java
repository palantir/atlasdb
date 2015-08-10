package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.Iterator;

import com.google.common.collect.ForwardingIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.palantir.common.base.ClosableIterator;

public final class ClosablePeekingIterator<T> extends ForwardingIterator<T> implements
        ClosableIterator<T>, PeekingIterator<T> {

    public static <V> ClosablePeekingIterator<V> of(ClosableIterator<V> it) {
        return new ClosablePeekingIterator<V>(it);
    }

    private final ClosableIterator<T> ci;
    private final PeekingIterator<T> pi;

    private ClosablePeekingIterator(ClosableIterator<T> ci) {
        this.ci = ci;
        this.pi = Iterators.peekingIterator(ci);
    }

    @Override
    protected Iterator<T> delegate() {
        // This takes care of next(), hasNext() and remove().
        // All of these should be handled by the peeking
        // iterator.
        return pi;
    }

    @Override
    public T peek() {
        return pi.peek();
    }

    @Override
    public void close() {
        ci.close();
    }

}
