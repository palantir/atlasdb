/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.invariants;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponse;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import org.junit.Test;

public class TimeLockActivityCheckerTest {
    private static final String CLIENT = "client";

    private final ConjureTimelockService timelockRpcClient = mock(ConjureTimelockService.class);
    private final TimeLockActivityChecker timeLockActivityChecker = new TimeLockActivityChecker(timelockRpcClient);

    @Test
    public void ifGetFreshTimestampReturnsTheValueIsPropagated() {
        when(timelockRpcClient.getFreshTimestamps(any(), eq(CLIENT), eq(ConjureGetFreshTimestampsRequest.of(1))))
                .thenReturn(ConjureGetFreshTimestampsResponse.of(8L, 8L));
        assertThat(timeLockActivityChecker.getFreshTimestampFromNodeForClient(CLIENT)).hasValue(8L);
    }

    @Test
    public void ifGetFreshTimestampThrowsReturnsEmpty() {
        when(timelockRpcClient.getFreshTimestamps(any(), eq(CLIENT), any())).thenThrow(new RuntimeException());
        assertThat(timeLockActivityChecker.getFreshTimestampFromNodeForClient(CLIENT)).isEmpty();
    }
}
