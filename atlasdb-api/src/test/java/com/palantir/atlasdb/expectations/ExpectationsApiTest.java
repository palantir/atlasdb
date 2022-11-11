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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.transaction.api.expectations.ExpectationsConfig;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableExpectationsConfig;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableKvsCallReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.KvsCallReadInfo;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ExpectationsApiTest {
    public static final String SMALLER_NAME = "test1";
    public static final String LARGER_NAME = "test2";
    public static final KvsCallReadInfo SMALLER_NAME_10 = ImmutableKvsCallReadInfo.of(SMALLER_NAME, 10);
    public static final KvsCallReadInfo SMALLER_NAME_20 = ImmutableKvsCallReadInfo.of(SMALLER_NAME, 20);
    public static final KvsCallReadInfo LARGER_NAME_10 = ImmutableKvsCallReadInfo.of(LARGER_NAME, 10);
    public static final KvsCallReadInfo LARGER_NAME_20 = ImmutableKvsCallReadInfo.of(LARGER_NAME, 20);

    public static final ExpectationsConfig DEFAULT_EXPECTATIONS_CONFIG =
            ImmutableExpectationsConfig.builder().build();

    @Test
    public void testKvsCallReadInfoComparator() {
        assertThat(SMALLER_NAME_10).isLessThan(SMALLER_NAME_20);
        assertThat(SMALLER_NAME_20).isGreaterThan(SMALLER_NAME_10);

        assertThat(LARGER_NAME_10).isLessThan(LARGER_NAME_20);
        assertThat(LARGER_NAME_20).isGreaterThan(LARGER_NAME_10);

        assertThat(LARGER_NAME_10).isLessThan(SMALLER_NAME_20);
        assertThat(SMALLER_NAME_20).isGreaterThan(LARGER_NAME_10);

        assertThat(SMALLER_NAME_10).isLessThan(LARGER_NAME_20);
        assertThat(LARGER_NAME_20).isGreaterThan(SMALLER_NAME_10);

        assertThat(SMALLER_NAME_10.compareTo(
                        ImmutableKvsCallReadInfo.builder().from(SMALLER_NAME_10).build()))
                .isEqualTo(0);
        assertThat(SMALLER_NAME_20.compareTo(
                        ImmutableKvsCallReadInfo.builder().from(SMALLER_NAME_20).build()))
                .isEqualTo(0);
        assertThat(LARGER_NAME_10.compareTo(
                        ImmutableKvsCallReadInfo.builder().from(LARGER_NAME_10).build()))
                .isEqualTo(0);
        assertThat(LARGER_NAME_20.compareTo(
                        ImmutableKvsCallReadInfo.builder().from(LARGER_NAME_20).build()))
                .isEqualTo(0);
    }

    @Test
    public void testExpectationsConfigValidation() {
        assertThatCode(() -> ImmutableExpectationsConfig.builder()
                        .from(DEFAULT_EXPECTATIONS_CONFIG)
                        .transactionName(StringUtils.repeat('t', ExpectationsConfig.MAXIMUM_NAME_SIZE))
                        .build())
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> ImmutableExpectationsConfig.builder()
                        .from(DEFAULT_EXPECTATIONS_CONFIG)
                        .transactionName(StringUtils.repeat('t', ExpectationsConfig.MAXIMUM_NAME_SIZE + 1))
                        .build())
                .isInstanceOf(SafeIllegalArgumentException.class);
    }
}
