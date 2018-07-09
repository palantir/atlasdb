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
package com.palantir.atlasdb.config;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class TimeLockClientConfigTest {
    private static final String CLIENT = "testClient";

    @Test
    public void canCreateWithoutClientSpecified() {
        ImmutableTimeLockClientConfig.builder()
                .build();
    }

    @Test
    public void tmelockClientCannotBeAnEmptyString() {
        assertThatThrownBy(() -> ImmutableTimeLockClientConfig
                .builder()
                .client("")
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .satisfies((exception) ->
                        assertThat(exception.getMessage(), containsString("Timelock client string cannot be empty")));
    }

    @Test(expected = IllegalStateException.class)
    public void throwsWhenReadingClientWithoutClientSpecified() {
        TimeLockClientConfig config = ImmutableTimeLockClientConfig.builder()
                .build();
        config.getClientOrThrow();
    }
}
