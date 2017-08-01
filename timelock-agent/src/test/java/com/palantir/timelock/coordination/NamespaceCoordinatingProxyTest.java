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

package com.palantir.timelock.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.timelock.partition.Assignment;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

public class NamespaceCoordinatingProxyTest {
    private static final String CLIENT = "client";
    private static final String LOCALHOST = "me";
    private static final Assignment ACTIVE_ASSIGNMENT = Assignment.builder().addMapping(CLIENT, LOCALHOST).build();
    private static final Assignment INACTIVE_ASSIGNMENT = Assignment.builder().addMapping(CLIENT, "other").build();
    private static final SequenceAndAssignment ACTIVE_SEQ_ASSIGNMENT =
            ImmutableSequenceAndAssignment.of(1L, ACTIVE_ASSIGNMENT);
    private static final SequenceAndAssignment INACTIVE_SEQ_ASSIGNMENT =
            ImmutableSequenceAndAssignment.of(1L, INACTIVE_ASSIGNMENT);

    private final CoordinationService coordinationService = mock(CoordinationService.class);
    private final AtomicLong creationCounter = new AtomicLong();
    private final TimestampService timestampService = NamespaceCoordinatingProxy.newProxyInstance(
            TimestampService.class,
            () -> {
                creationCounter.incrementAndGet();
                return new InMemoryTimestampService();
            },
            CLIENT,
            LOCALHOST,
            coordinationService);

    @Test
    public void throwsNotCurrentLeaderExceptionIfWeDontKnowThePartition() {
        when(coordinationService.getCoordinatedValue()).thenReturn(INACTIVE_SEQ_ASSIGNMENT);
        assertThatThrownBy(timestampService::getFreshTimestamp)
                .isInstanceOf(NotCurrentLeaderException.class);
    }

    @Test
    public void throwsNotCurrentLeaderExceptionIfWeAreNotPartOfTheKnownPartition() {
        when(coordinationService.getCoordinatedValue()).thenReturn(INACTIVE_SEQ_ASSIGNMENT);
        assertThatThrownBy(timestampService::getFreshTimestamp)
                .isInstanceOf(NotCurrentLeaderException.class);
    }

    @Test
    public void callsRealMethodIfWeArePartOfTheKnownPartition() {
        when(coordinationService.getCoordinatedValue()).thenReturn(ACTIVE_SEQ_ASSIGNMENT);
        long ts1 = timestampService.getFreshTimestamp();
        long ts2 = timestampService.getFreshTimestamp();
        assertThat(ts1).isLessThan(ts2);
    }

    @Test
    public void recreatesClassIfWeLoseLeadership() {
        when(coordinationService.getCoordinatedValue()).thenReturn(ACTIVE_SEQ_ASSIGNMENT);
        timestampService.getFreshTimestamp();
        when(coordinationService.getCoordinatedValue()).thenReturn(INACTIVE_SEQ_ASSIGNMENT);
        assertThatThrownBy(timestampService::getFreshTimestamp)
                .isInstanceOf(NotCurrentLeaderException.class);
        when(coordinationService.getCoordinatedValue()).thenReturn(ACTIVE_SEQ_ASSIGNMENT);
        timestampService.getFreshTimestamp();
        assertThat(creationCounter.get()).isEqualTo(2L);
    }
}
