/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.logsafe.SafeArg;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ThrowableInstanceNeverThrown")
public class RetryLimitReachedExceptionTest {
    private static final Exception RUNTIME = new RuntimeException();
    private static final Exception ATLAS_DEPENDENCY = new AtlasDbDependencyException(RUNTIME);
    private static final Exception GENERIC = new Exception();

    @Test
    public void noMatches() {
        RetryLimitReachedException exception = new RetryLimitReachedException(
                ImmutableList.of(RUNTIME, ATLAS_DEPENDENCY, GENERIC), ImmutableMap.of("host1", 1));
        Assertions.assertThat(exception.suppressed(IllegalStateException.class)).isFalse();
    }

    @Test
    public void exactMatch() {
        RetryLimitReachedException exception = new RetryLimitReachedException(
                ImmutableList.of(RUNTIME, ATLAS_DEPENDENCY, GENERIC), ImmutableMap.of("host1", 1));
        Assertions.assertThat(exception.suppressed(RuntimeException.class)).isTrue();
    }

    @Test
    public void superMatch() {
        RetryLimitReachedException exception = new RetryLimitReachedException(
                ImmutableList.of(ATLAS_DEPENDENCY, GENERIC), ImmutableMap.of("host1", 1));
        Assertions.assertThat(exception.suppressed(RuntimeException.class)).isTrue();
    }

    @Test
    public void testArgs() {
        Map<String, Integer> hostsTried = ImmutableMap.of("host1", 1);
        RetryLimitReachedException exception =
                new RetryLimitReachedException(ImmutableList.of(RUNTIME, ATLAS_DEPENDENCY, GENERIC), hostsTried);
        Assertions.assertThat(exception.getArgs()).contains(SafeArg.of("hostsToNumAttemptsTried", hostsTried));
        Assertions.assertThat(exception.getArgs()).contains(SafeArg.of("numRetries", 3));
    }
}
