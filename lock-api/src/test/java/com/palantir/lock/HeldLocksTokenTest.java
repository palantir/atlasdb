package com.palantir.lock;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSortedMap;

public class HeldLocksTokenTest {
	@Test
	public void testSerde() throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		
		LockDescriptor ld1 = new LockDescriptor("hello".getBytes(StandardCharsets.UTF_8));
		LockDescriptor ld2 = new LockDescriptor("other".getBytes(StandardCharsets.UTF_8));
		SortedLockCollection<LockDescriptor> lockMap = LockCollections.of(ImmutableSortedMap.of(ld1, LockMode.READ, ld2, LockMode.WRITE));
		HeldLocksToken heldLocksToken = new HeldLocksToken(BigInteger.valueOf(123), LockClient.ANONYMOUS, 1l, 1l, lockMap, SimpleTimeDuration.of(1, TimeUnit.SECONDS), 1l);
		
		String serialized = mapper.writeValueAsString(heldLocksToken);
		HeldLocksToken deserialized = mapper.readValue(serialized, HeldLocksToken.class);
		
		assertThat(deserialized, is(heldLocksToken));
	}
}
