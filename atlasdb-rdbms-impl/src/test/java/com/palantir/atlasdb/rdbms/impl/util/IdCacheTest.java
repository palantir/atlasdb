package com.palantir.atlasdb.rdbms.impl.util;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsSchemaVersion;
import com.palantir.atlasdb.rdbms.impl.service.AtlasRdbmsImpl;
import com.palantir.atlasdb.rdbms.impl.service.AtlasRdbmsTestUtils;
import com.palantir.atlasdb.rdbms.impl.util.IdCache;
import com.palantir.atlasdb.rdbms.impl.util.IdCacheImpl;

public class IdCacheTest {

    private AtlasRdbmsImpl db;
    private Set<String> createdSequences;

    @Before
    public void setUp() throws Exception {
        createdSequences = Sets.newHashSet();
        db = AtlasRdbmsTestUtils.getTestDb();
    }

    @After
    public void tearDown() throws Exception {
        for (String sequence : createdSequences) {
            dropSequence(sequence);
        }
        db.shutdown();
    }

    private void createSequence(String name, long start, long increment) throws Exception {
        AtlasRdbmsSchemaVersion current = db.getDbSchemaVersion();
        db.performSchemaMigration(current, new AtlasRdbmsSchemaVersion(current.getMajorVersion() + 1, 0),
                Lists.newArrayList("CREATE SEQUENCE " + name + " INCREMENT BY " + increment + " START WITH " + start));
        createdSequences.add(name);
    }

    private void dropSequence(String name) throws Exception {
        AtlasRdbmsSchemaVersion current = db.getDbSchemaVersion();
        db.performSchemaMigration(current, new AtlasRdbmsSchemaVersion(current.getMajorVersion() + 1, 0),
                Lists.newArrayList("DROP SEQUENCE " + name));
    }

    @Test
    public void testIncrementByOneSequence() throws Exception {
        String sequence = "atlas_test_sequence";
        createSequence(sequence, 0, 1);
        IdCache idCache = new IdCacheImpl(db, Collections.singletonMap(sequence, 1L));
        Iterable<Long> ids = idCache.getIds(sequence, 5);
        List<Long> expected = Lists.newArrayList(0L, 1L, 2L, 3L, 4L);
        Assert.assertEquals(expected, Lists.newArrayList(ids));

        ids = idCache.getIds(sequence, 3);
        expected = Lists.newArrayList(5L, 6L, 7L);
        Assert.assertEquals(expected, Lists.newArrayList(ids));
    }

    @Test
    public void testMultipleSequences() throws Exception {
        String seq2 = "atlast_test_sequence2";
        String seq3 = "atlast_test_sequence3";
        String seq5 = "atlast_test_sequence5";
        String seq7 = "atlast_test_sequence7";
        createSequence(seq2, 0, 2);
        createSequence(seq3, 0, 3);
        createSequence(seq5, 0, 5);
        createSequence(seq7, 0, 7);
        Map<String, Long> sequences = Maps.newHashMap();
        sequences.put(seq2, 2L);
        sequences.put(seq3, 3L);
        sequences.put(seq5, 5L);
        sequences.put(seq7, 7L);
        IdCache idCache = new IdCacheImpl(db, sequences);

        Iterable<Long> ids = idCache.getIds(seq2, 5);
        List<Long> expected = Lists.newArrayList(0L, 1L, 2L, 3L, 4L);
        Assert.assertEquals(expected, Lists.newArrayList(ids));

        ids = idCache.getIds(seq3, 5);
        Assert.assertEquals(expected, Lists.newArrayList(ids));

        ids = idCache.getIds(seq5, 5);
        Assert.assertEquals(expected, Lists.newArrayList(ids));

        ids = idCache.getIds(seq7, 5);
        Assert.assertEquals(expected, Lists.newArrayList(ids));
    }

    @Test
    public void testInvalidate() throws Exception {
        String sequence = "atlas_test_sequence";
        createSequence(sequence, 0, 10);
        IdCache idCache = new IdCacheImpl(db, Collections.singletonMap(sequence, 10L));
        Iterable<Long> ids = idCache.getIds(sequence, 5);
        List<Long> expected = Lists.newArrayList(0L, 1L, 2L, 3L, 4L);
        Assert.assertEquals(expected, Lists.newArrayList(ids));

        idCache.invalidate();

        ids = idCache.getIds(sequence, 3);
        expected = Lists.newArrayList(10L, 11L, 12L);
        Assert.assertEquals(expected, Lists.newArrayList(ids));
    }

    @Test
    public void testMultithreadedUse() throws Exception {
        final Random r = new Random();
        final String sequence = "atlas_test_sequence";
        createSequence(sequence, 0, 10);
        final IdCache idCache = new IdCacheImpl(db, Collections.singletonMap(sequence, 10L));

        ExecutorService executor = Executors.newFixedThreadPool(100);
        Collection<Future<Set<Long>>> futures = Lists.newArrayList();
        for (int i = 0; i < 10000; i++) {
            futures.add(executor.submit(new Callable<Set<Long>>() {
                @Override
                public Set<Long> call() throws Exception {
                    int numIds = 5 + r.nextInt(5);
                    Iterable<Long> ids = idCache.getIds(sequence, numIds);
                    Set<Long> idSet = Sets.newHashSet(ids);
                    if (idSet.size() != numIds) {
                        throw new AssertionError();
                    }
                    return idSet;
                }
            }));
        }
        executor.shutdown();
        Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

        int numIds = 0;
        Set<Long> allIds = Sets.newHashSet();
        for (Future<Set<Long>> future : futures) {
            try {
                Set<Long> ids = future.get();
                numIds += ids.size();
                allIds.addAll(ids);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof AssertionError) {
                    Assert.fail();
                }
            }
        }

        Assert.assertEquals(numIds, allIds.size());
        Set<Long> expectedIds = Sets.newHashSet();
        for (long i = 0; i < numIds; i++) {
            expectedIds.add(i);
        }
        Assert.assertEquals(expectedIds, allIds);
    }

    @Test
    public void testInvalidInput() throws Exception {
        final String sequence = "atlas_test_sequence";
        createSequence(sequence, 0, 10);
        final IdCache idCache = new IdCacheImpl(db, Collections.singletonMap(sequence, 10L));

        try {
            idCache.getIds(sequence, 0);
            Assert.fail("Must request a positive number of IDs");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Must request a positive number of IDs", e.getMessage());
        }

        try {
            idCache.getIds(sequence, -1);
            Assert.fail("Must request a positive number of IDs");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Must request a positive number of IDs", e.getMessage());
        }

        String fakeSequence = sequence + "_fake";
        try {
            idCache.getIds(fakeSequence, 1);
            Assert.fail("Sequence not recognized: " + fakeSequence);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Sequence not recognized: " + fakeSequence, e.getMessage());
        }

        Assert.assertEquals(0, Iterables.getOnlyElement(idCache.getIds(sequence, 1)).longValue());
    }
}
