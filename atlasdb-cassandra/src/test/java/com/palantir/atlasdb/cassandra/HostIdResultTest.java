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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.Set;
import org.junit.Test;

public final class HostIdResultTest {

    @Test
    public void hostIdsMustBeEmptyWhenSoftFailure() {
        assertThatThrownBy(() -> HostIdResult.builder()
                        .type(HostIdResult.Type.SOFT_FAILURE)
                        .hostIds(Set.of("foo"))
                        .build())
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void hostIdsMustBeEmptyWhenHardFailure() {
        assertThatThrownBy(() -> HostIdResult.builder()
                        .type(HostIdResult.Type.HARD_FAILURE)
                        .hostIds(Set.of("foo"))
                        .build())
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("It is expected that no hostIds should be present when there is a failure.");
    }

    @Test
    public void hostIdsMustNotBeEmptyWhenSuccess() {
        assertThatThrownBy(() -> HostIdResult.builder()
                        .type(HostIdResult.Type.SUCCESS)
                        .hostIds(Set.of())
                        .build())
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageNotContaining(
                        "It is expected that there should be at least one host id if the result is successful.");
    }

    @Test
    public void canBuildWhenHostIdsSpecifiedAndSuccess() {
        assertThatCode(() -> HostIdResult.builder()
                        .type(HostIdResult.Type.SUCCESS)
                        .hostIds(Set.of("foo"))
                        .build())
                .doesNotThrowAnyException();
    }
}
