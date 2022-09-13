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

package com.palantir.atlasdb.atomic.mcas;

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.Unsafe;
import java.util.Optional;
import org.immutables.value.Value;

@Unsafe
@Value.Immutable
interface CasResponse {
    boolean successful();

    Optional<Exception> exception();

    @Value.Check
    default void check() {
        Preconditions.checkState(
                successful() || exception().isPresent(),
                "The response can either be successful OR fail with exception.");
    }

    static CasResponse success() {
        return ImmutableCasResponse.builder().successful(true).build();
    }

    static CasResponse failure(Exception ex) {
        return ImmutableCasResponse.builder().successful(false).exception(ex).build();
    }
}
