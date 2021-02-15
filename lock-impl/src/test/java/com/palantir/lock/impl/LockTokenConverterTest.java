/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.lock.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.v2.LockToken;
import java.math.BigInteger;
import java.util.Random;
import org.junit.Test;

public class LockTokenConverterTest {

    private static final Random RANDOM = new Random(123);

    @Test
    public void convertsBigIntegersCorrectly() {
        int iterations = 100_000;
        int bitLength = LockServiceImpl.RANDOM_BIT_COUNT;
        for (int i = 0; i < iterations; i++) {
            BigInteger randomBigInteger = new BigInteger(bitLength, RANDOM);
            LockRefreshToken lockRefreshToken = new LockRefreshToken(randomBigInteger, Long.MIN_VALUE);
            assertConversionFromAndToLegacyPreservesId(lockRefreshToken);
        }
    }

    @Test
    public void convertsNegativeBigIntegersCorrectly() {
        int iterations = 100_000;
        int bitLength = LockServiceImpl.RANDOM_BIT_COUNT;
        for (int i = 0; i < iterations; i++) {
            BigInteger randomBigInteger = new BigInteger(bitLength, RANDOM).negate();
            LockRefreshToken lockRefreshToken = new LockRefreshToken(randomBigInteger, Long.MIN_VALUE);
            assertConversionFromAndToLegacyPreservesId(lockRefreshToken);
        }
    }

    @Test
    public void throwsIfBigIntegerHasMoreThan127Bits() {
        BigInteger bigInt = BigInteger.valueOf(2).pow(127);

        LockRefreshToken token = new LockRefreshToken(bigInt, 0L);

        assertThatThrownBy(() -> LockTokenConverter.toTokenV2(token))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("too many bits");
    }

    private void assertConversionFromAndToLegacyPreservesId(LockRefreshToken lockRefreshToken) {
        LockToken tokenV2 = LockTokenConverter.toTokenV2(lockRefreshToken);
        LockRefreshToken reconverted = LockTokenConverter.toLegacyToken(tokenV2);

        assertThat(reconverted).isEqualTo(lockRefreshToken);
    }
}
