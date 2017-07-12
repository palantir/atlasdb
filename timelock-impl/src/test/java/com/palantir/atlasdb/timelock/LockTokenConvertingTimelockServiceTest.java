/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import java.util.UUID;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.lock.v2.LockTokenV2;

public class LockTokenConvertingTimelockServiceTest {
    private static final UUID TEST_UUID = new UUID(12345, 67890);

    @Test
    public void castToAdapterPreservesRequestId() {
        LockTokenV2 tokenV2 = LockTokenV2.of(TEST_UUID);
        LockTokenV2 legacyVersion = LockTokenConvertingTimelockService.castToAdapter(tokenV2);
        assertThat(legacyVersion.getRequestId()).isEqualTo(tokenV2.getRequestId());
    }

    @Test
    public void makeSerializableMakesLockTokensSerializable() throws JsonProcessingException {
        LockTokenV2 tokenV2 = new LegacyTimelockService.LockRefreshTokenV2Adapter(
                new LockRefreshToken(BigInteger.ZERO, Long.MIN_VALUE));
        LockTokenV2 serializableToken = LockTokenConvertingTimelockService.makeSerializable(tokenV2);
        new ObjectMapper().writeValueAsString(serializableToken);
    }

    @Test
    public void makeSerializablePreservesTokenId() {
        LockTokenV2 tokenV2 = new LegacyTimelockService.LockRefreshTokenV2Adapter(
                new LockRefreshToken(BigInteger.ZERO, Long.MIN_VALUE));
        LockTokenV2 serializableToken = LockTokenConvertingTimelockService.makeSerializable(tokenV2);
        assertThat(serializableToken.getRequestId()).isEqualTo(new UUID(0L, 0L));
    }

    @Test
    public void makeSerializableWorksWithNumbersBeyondSixtyFourBits() {
        BigInteger bigInteger = new BigInteger("2")
                .pow(64)
                .add(BigInteger.ONE); // This returns (1 << 65) + 1
        LockTokenV2 tokenV2 = new LegacyTimelockService.LockRefreshTokenV2Adapter(
                new LockRefreshToken(bigInteger, Long.MIN_VALUE));
        LockTokenV2 serializableToken = LockTokenConvertingTimelockService.makeSerializable(tokenV2);
        assertThat(serializableToken.getRequestId()).isEqualTo(new UUID(1L, 1L));
    }

    @Test
    public void identifiersPreservedOnRepeatedConversions() {
        LockTokenV2 tokenV2 = LockTokenV2.of(TEST_UUID);
        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            tokenV2 = LockTokenConvertingTimelockService.castToAdapter(tokenV2);
            assertThat(tokenV2.getRequestId()).isEqualTo(TEST_UUID);
            tokenV2 = LockTokenConvertingTimelockService.makeSerializable(tokenV2);
            assertThat(tokenV2.getRequestId()).isEqualTo(TEST_UUID);
        }
    }
}
