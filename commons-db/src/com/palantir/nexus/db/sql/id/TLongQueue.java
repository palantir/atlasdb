package com.palantir.nexus.db.sql.id;

import java.util.NoSuchElementException;

/**
 * A resizable, array-backed queue of long primitives optimized for fast adding and removal of
 * arrays.
 * <p>The name of this class is intentionally similar to the class names in GNU Trove.
 * @author eporter
 */
public class TLongQueue {

    private long [] data;
    // The index of the first entry in the array with data.
    private int head;
    // How many entries have data.
    private int size;

    protected static final int DEFAULT_CAPACITY = 10;

    public TLongQueue() {
        this(DEFAULT_CAPACITY);
    }

    public TLongQueue(int capacity) {
        data = new long[capacity];
        head = 0;
        size = 0;
    }

    private void ensureCapacity(int capacity) {
        if (capacity > data.length) {
            int oldSize = size;
            int newCap = Math.max(data.length << 1, capacity);
            long [] tmp = new long [newCap];
            remove(tmp, oldSize);
            head = 0;
            size = oldSize;
            data = tmp;
        }
    }

    public void add(long val) {
        ensureCapacity(size + 1);
        data[(head + size) % data.length] = val;
        size++;
    }

    /**
     * Adds the values in the array <tt>vals</tt> to the end of the
     * queue, in order.
     *
     * @param vals an <code>long[]</code> value
     */
    public void add(long[] vals) {
        add(vals, vals.length);
    }

    /**
     * Adds a subset of the values in the array <tt>vals</tt> to the
     * end of the queue, in order.
     *
     * @param vals an <code>long[]</code> value
     * @param length the number of values to copy.
     */
    public void add(long [] vals, int length) {
        ensureCapacity(size + length);
        int insertPos = (head + size) % data.length;
        int firstCopyLen = Math.min(length, data.length - insertPos);
        System.arraycopy(vals, 0, data, insertPos, firstCopyLen);
        if (firstCopyLen < length) {
            System.arraycopy(vals, firstCopyLen, data, 0, length - firstCopyLen);
        }
        size += length;
    }

    public long remove() {
        if (size == 0) {
            throw new NoSuchElementException("The queue is empty"); //$NON-NLS-1$
        }
        long toReturn = data[head];
        head = (head + 1) % data.length;
        size--;
        return toReturn;
    }

    public long [] remove(int length) {
        long [] vals = new long [length];
        remove(vals);
        return vals;
    }

    public void remove(long [] vals) {
        remove(vals, vals.length);
    }

    // Package private access for testing and internal use.
    void remove(long [] vals, int length) {
        if (size < length) {
            throw new NoSuchElementException("The queue has fewer than " + length + " elements."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        int firstCopyLen = Math.min(length, data.length - head);
        System.arraycopy(data, head, vals, 0, firstCopyLen);
        if (firstCopyLen < length) {
            System.arraycopy(data, 0, vals, firstCopyLen, length - firstCopyLen);
        }
        head = (head + length) % data.length;
        size -= length;
    }

    public int size() {
        return size;
    }
}
