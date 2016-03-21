/**
 * Copyright 2015 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
		
		// manually assert each field is equal, because equality in HeldLocksToken is just comparing the token id
		assertThat(deserialized.getClient(), is(heldLocksToken.getClient()));
		assertThat(deserialized.getCreationDateMs(), is(heldLocksToken.getCreationDateMs()));
		assertThat(deserialized.getExpirationDateMs(), is(heldLocksToken.getExpirationDateMs()));
		assertThat(deserialized.getLockRefreshToken(), is(heldLocksToken.getLockRefreshToken()));
		assertThat(deserialized.getLocks(), is(heldLocksToken.getLocks()));
		assertThat(deserialized.getLockTimeout(), is(heldLocksToken.getLockTimeout()));
		assertThat(deserialized.getTokenId(), is(heldLocksToken.getTokenId()));
		assertThat(deserialized.getVersionId(), is(heldLocksToken.getVersionId()));
	}
}
