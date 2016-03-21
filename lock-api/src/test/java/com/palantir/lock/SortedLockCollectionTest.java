package com.palantir.lock;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSortedMap;

public class SortedLockCollectionTest {
    @Test
    public void testSerde() throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        LockDescriptor ld1 = new LockDescriptor("hello".getBytes(StandardCharsets.UTF_8));
        LockDescriptor ld2 = new LockDescriptor("other".getBytes(StandardCharsets.UTF_8));

        SortedLockCollection<LockDescriptor> locks = LockCollections.of(ImmutableSortedMap.of(ld1, LockMode.READ, ld2, LockMode.WRITE));
        
        String serialized = mapper.writeValueAsString(locks);
        SortedLockCollection<LockDescriptor> deserialized = mapper.readValue(serialized, SortedLockCollection.class);
        
        Assert.assertThat(deserialized, Matchers.is(locks));
    }
}
