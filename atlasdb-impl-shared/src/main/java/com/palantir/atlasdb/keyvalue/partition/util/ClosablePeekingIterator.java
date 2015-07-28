package com.palantir.atlasdb.keyvalue.partition.util;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.palantir.common.base.ClosableIterator;

public final class ClosablePeekingIterator<T> implements ClosableIterator<T>, PeekingIterator<T> {

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
    public boolean hasNext() {
        return pi.hasNext();
    }

    @Override
    public T next() {
        return pi.next();
    }

    @Override
    public void remove() {
        pi.remove();

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
