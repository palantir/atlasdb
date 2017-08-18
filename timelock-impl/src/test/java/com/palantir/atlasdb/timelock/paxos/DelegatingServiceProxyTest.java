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

package com.palantir.atlasdb.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

public class DelegatingServiceProxyTest {
    @Test
    public void throwsWhenNotInitialized() {
        TimestampService timestampService = DelegatingServiceProxy.newProxyInstance(new AtomicReference<>(),
                TimestampService.class);
        assertThatThrownBy(() -> timestampService.getFreshTimestamp()).isInstanceOf(ServiceNotAvailableException.class).hasMessage("This service is not ready yet.");
    }

    @Test
    public void returnsCorrectlyWhenInitialized() {
        TimestampService timestampService = DelegatingServiceProxy.newProxyInstance(new AtomicReference<>(new InMemoryTimestampService()), TimestampService.class);
        long firstTimestamp = timestampService.getFreshTimestamp();
        long secondTimestamp = timestampService.getFreshTimestamp();
        assertThat(firstTimestamp).isLessThan(secondTimestamp);
    }
}
