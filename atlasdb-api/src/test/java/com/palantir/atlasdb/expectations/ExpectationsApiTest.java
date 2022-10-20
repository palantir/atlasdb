/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.expectations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.palantir.atlasdb.transaction.api.expectations.ExpectationsConfig;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableExpectationsConfig;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableKvsCallReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.KvsCallReadInfo;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ExpectationsApiTest {
    public static final String SMALLER_NAME = "test1";
    public static final String LARGER_NAME = "test2";
    public static final KvsCallReadInfo SMALLER_NAME_10 = ImmutableKvsCallReadInfo.builder()
            .bytesRead(10)
            .methodName(SMALLER_NAME)
            .build();
    public static final KvsCallReadInfo SMALLER_NAME_20 = ImmutableKvsCallReadInfo.builder()
            .bytesRead(20)
            .methodName(SMALLER_NAME)
            .build();
    public static final KvsCallReadInfo LARGER_NAME_10 = ImmutableKvsCallReadInfo.builder()
            .bytesRead(10)
            .methodName(LARGER_NAME)
            .build();
    public static final KvsCallReadInfo LARGER_NAME_20 = ImmutableKvsCallReadInfo.builder()
            .bytesRead(20)
            .methodName(LARGER_NAME)
            .build();

    public static final ExpectationsConfig DEFAULT_EXPECTATIONS_CONFIG =
            ImmutableExpectationsConfig.builder().build();

    @Test
    public void testKvsCallReadInfoComparator() {
        assertTrue(SMALLER_NAME_10.compareTo(SMALLER_NAME_20) < 0);
        assertTrue(SMALLER_NAME_20.compareTo(SMALLER_NAME_10) > 0);

        assertTrue(LARGER_NAME_10.compareTo(LARGER_NAME_20) < 0);
        assertTrue(LARGER_NAME_20.compareTo(LARGER_NAME_10) > 0);

        assertTrue(LARGER_NAME_10.compareTo(SMALLER_NAME_20) < 0);
        assertTrue(SMALLER_NAME_20.compareTo(LARGER_NAME_10) > 0);

        assertTrue(SMALLER_NAME_10.compareTo(LARGER_NAME_20) < 0);
        assertTrue(LARGER_NAME_20.compareTo(SMALLER_NAME_10) > 0);

        assertEquals(
                0,
                SMALLER_NAME_10.compareTo(
                        ImmutableKvsCallReadInfo.builder().from(SMALLER_NAME_10).build()));
        assertEquals(
                0,
                SMALLER_NAME_20.compareTo(
                        ImmutableKvsCallReadInfo.builder().from(SMALLER_NAME_20).build()));
        assertEquals(
                0,
                LARGER_NAME_10.compareTo(
                        ImmutableKvsCallReadInfo.builder().from(LARGER_NAME_10).build()));
        assertEquals(
                0,
                LARGER_NAME_20.compareTo(
                        ImmutableKvsCallReadInfo.builder().from(LARGER_NAME_20).build()));
    }

    @Test
    public void testExpectationsConfigValidation() {
        ExpectationsConfig validConfig = ImmutableExpectationsConfig.builder()
                .from(DEFAULT_EXPECTATIONS_CONFIG)
                .transactionName(StringUtils.repeat('t', ExpectationsConfig.MAXIMUM_NAME_SIZE))
                .build();

        assertThrows(IllegalArgumentException.class, () -> ImmutableExpectationsConfig.builder()
                .from(DEFAULT_EXPECTATIONS_CONFIG)
                .transactionName(StringUtils.repeat('t', ExpectationsConfig.MAXIMUM_NAME_SIZE + 1))
                .build());
    }
}
