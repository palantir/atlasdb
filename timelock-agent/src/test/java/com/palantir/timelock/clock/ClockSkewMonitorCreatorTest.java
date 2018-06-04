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

package com.palantir.timelock.clock;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Optional;
import java.util.function.Consumer;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.clock.ClockServiceImpl;

public class ClockSkewMonitorCreatorTest {
    private final Consumer<Object> registrar = mock(Consumer.class);

    @Test
    public void registersClockServiceImpl() {
        ClockSkewMonitorCreator clockSkewMonitorCreator = new ClockSkewMonitorCreator(
                null, ImmutableSet.of("foo:1"),
                Optional.empty(),
                registrar);
        clockSkewMonitorCreator.registerClockServices();
        verify(registrar).accept(any(ClockServiceImpl.class));
    }
}
