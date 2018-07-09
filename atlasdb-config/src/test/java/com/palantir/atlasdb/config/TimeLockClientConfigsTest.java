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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class TimeLockClientConfigsTest {
    private static final String CLIENT_1 = "bar";
    private static final String CLIENT_2 = "baz";

    private static final TimeLockClientConfig CONFIG_WITHOUT_CLIENT = ImmutableTimeLockClientConfig.builder()
            .build();
    private static final TimeLockClientConfig CONFIG_WITH_CLIENT = ImmutableTimeLockClientConfig.builder()
            .client(CLIENT_2)
            .build();

    @Test
    public void canCopyAddingClient() {
        TimeLockClientConfig newConfig =
                TimeLockClientConfigs.copyWithClient(CONFIG_WITHOUT_CLIENT, CLIENT_1);
        assertThat(newConfig.getClientOrThrow()).isEqualTo(CLIENT_1);
    }

    @Test
    public void canCopyReplacingClient() {
        TimeLockClientConfig newConfig =
                TimeLockClientConfigs.copyWithClient(CONFIG_WITH_CLIENT, CLIENT_1);
        assertThat(newConfig.getClientOrThrow()).isEqualTo(CLIENT_1);
    }
}
