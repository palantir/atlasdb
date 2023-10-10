/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class IdSupportingHostIdResultTest {
    @Test
    public void throwsWhenCreatingWithSoftFailure() {
        assertThatThrownBy(() -> IdSupportingHostIdResult.wrap(HostIdResult.softFailure()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Soft failures are not ID supporting and thus not allowed.");
    }

    @Test
    public void canCreateWithSuccessAndRetrieveOriginal() {
        HostIdResult success = HostIdResult.success(ImmutableList.of("alice", "bob"));
        IdSupportingHostIdResult wrappedSuccess = IdSupportingHostIdResult.wrap(success);

        assertThat(wrappedSuccess.result()).isEqualTo(success);
    }

    @Test
    public void canCreateWithHardFailureAndRetrieveOriginal() {
        HostIdResult hardFailure = HostIdResult.hardFailure();
        IdSupportingHostIdResult wrappedSuccess = IdSupportingHostIdResult.wrap(hardFailure);

        assertThat(wrappedSuccess.result()).isEqualTo(hardFailure);
    }
}
