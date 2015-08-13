package com.palantir.atlasdb.rdbms.impl.util;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class IdRangeTest {

    private static final Random r = new Random();

    @Test
    public void testSize() {
        IdRange range = new IdRange(0, 0);
        Assert.assertEquals(0, range.size());

        range = new IdRange(1, 5);
        Assert.assertEquals(4, range.size());

        for (int i = 0; i < 1000; i++) {
            long start = r.nextInt(100000);
            long size = r.nextInt(100000);
            range = new IdRange(start, start + size);
            Assert.assertEquals(size, range.size());
        }
    }

    @Test
    public void testValues() throws Exception {
        IdRange range = new IdRange(0, 0);
        List<Long> ids = Lists.newArrayList(range);
        Assert.assertTrue(ids.isEmpty());

        range = new IdRange(1, 5);
        ids = Lists.newArrayList(range);
        Assert.assertEquals(Lists.newArrayList(1L, 2L, 3L, 4L), ids);

        for (int i = 0; i < 1000; i++) {
            long start = r.nextInt(100000);
            long size = r.nextInt(100000);
            range = new IdRange(start, start + size);
            ids = Lists.newArrayList(range);
            List<Long> expected = Lists.newArrayList();
            for (long j = start; j < start + size; j++) {
                expected.add(j);
            }
            Assert.assertEquals(expected, ids);
        }
    }

    @Test
    public void testStrip() throws Exception {
        IdRange range = new IdRange(1, 5);
        IdRange stripped = range.strip(2);
        Assert.assertEquals(Lists.newArrayList(1L, 2L), Lists.newArrayList(stripped));
        Assert.assertEquals(Lists.newArrayList(3L, 4L), Lists.newArrayList(range));

        stripped = range.strip(2);
        Assert.assertEquals(Lists.newArrayList(3L, 4L), Lists.newArrayList(stripped));
        Assert.assertEquals(0, range.size());

        IdRange emptyStrip = range.strip(0);
        Assert.assertEquals(Lists.newArrayList(3L, 4L), Lists.newArrayList(stripped));
        Assert.assertEquals(0, emptyStrip.size());

        for (int i = 0; i < 1000; i++) {
            long start = r.nextInt(100000);
            long size = r.nextInt(100000);
            long toStrip = r.nextInt((int)size);
            range = new IdRange(start, start + size);
            stripped = range.strip(toStrip);

            List<Long> expected = Lists.newArrayList();
            for (long j = start + toStrip; j < start + size; j++) {
                expected.add(j);
            }
            Assert.assertEquals(expected, Lists.newArrayList(range));

            expected = Lists.newArrayList();
            for (long j = start; j < start + toStrip; j++) {
                expected.add(j);
            }
            Assert.assertEquals(expected, Lists.newArrayList(stripped));
        }
    }

    @Test
    public void testInvalidInputs() throws Exception {
        try {
            new IdRange(1, 0);
            Assert.fail("Range end must be at or after range start");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Range end must be at or after range start", e.getMessage());
        }

        IdRange range = new IdRange(0, 1);

        try {
            Iterator<Long> iterator = range.iterator();
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(0, iterator.next().longValue());
            Assert.assertFalse(iterator.hasNext());
            iterator.next();
            Assert.fail("No ids remaining");
        } catch (IllegalStateException e) {
            Assert.assertEquals("No ids remaining", e.getMessage());
        }

        try {
            Assert.assertEquals(1, range.size());
            range.strip(2);
            Assert.fail("Cannot strip more IDs than are in this range");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Cannot strip more IDs than are in this range", e.getMessage());
        }

        try {
            range.strip(-1);
            Assert.fail("Cannot strip a negative number of IDs");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Cannot strip a negative number of IDs", e.getMessage());
        }
    }
}
