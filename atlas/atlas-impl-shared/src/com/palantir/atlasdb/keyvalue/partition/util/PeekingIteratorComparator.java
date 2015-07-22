package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.Comparator;

import com.google.common.collect.PeekingIterator;

public class PeekingIteratorComparator <T> implements Comparator<PeekingIterator<T>>{

    final Comparator<T> contentCmp;

    @Override
    public int compare(PeekingIterator<T> o1, PeekingIterator<T> o2) {
        if (o1.hasNext() && o2.hasNext()) {
            return contentCmp.compare(o1.peek(), o2.peek());
        }
        if (o1.hasNext()) {
            return -1;
        }
        if (o2.hasNext()) {
            return 1;
        }
        if (o1.equals(o2)) {
            return 0;
        }
        return Integer.compare(o1.hashCode(), o2.hashCode());
    }

    private PeekingIteratorComparator(Comparator<T> cmp) {
        contentCmp = cmp;
    }

    public static <T> PeekingIteratorComparator<T> WithComparator(Comparator<T> cmp) {
        return new PeekingIteratorComparator<T>(cmp);
    }

}
