/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock.atomix;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.channels.ByteChannel;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;
import javax.ws.rs.ServiceUnavailableException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.UncheckedExecutionException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.atomix.catalyst.concurrent.Futures;
import io.atomix.group.GroupMember;
import io.atomix.group.LocalMember;
import io.atomix.group.election.internal.GroupElection;
import io.atomix.group.internal.MembershipGroup;
import io.atomix.variables.DistributedValue;

public class InvalidatingLeaderProxyTest {
    private static final String TEST_VALUE = "testValue";
    private static final String LOCAL_MEMBER_ID = "localId";

    private static final DistributedValue<LeaderAndTerm> LEADER_INFO = mock(DistributedValue.class);
    private static final LocalMember LOCAL_MEMBER = mock(LocalMember.class);

    private final GroupElection election = new GroupElection(mock(MembershipGroup.class));
    private final AtomicString atomicString = InvalidatingLeaderProxy.create(
            LOCAL_MEMBER,
            LEADER_INFO,
            SimpleAtomicString::new,
            AtomicString.class);

    @BeforeClass
    public static void setupMocks() {
        when(LOCAL_MEMBER.id()).thenReturn(LOCAL_MEMBER_ID);
        when(LEADER_INFO.get()).thenReturn(null);
    }

    @Test
    public void shouldCallDelegateIfLeader() {
        setLeader(LOCAL_MEMBER_ID);
        assertCanReadAndWriteValue(atomicString);
    }

    @Test
    public void shouldBeUnableToCallDelegateIfNotLeader() {
        setLeader(null);
        assertThatThrownBy(atomicString::get).isInstanceOf(ServiceUnavailableException.class);
    }

    @Test
    public void shouldBeAbleToCallDelegateAgainOnRegainingLeadership() {
        setLeader(LOCAL_MEMBER_ID);
        assertCanReadAndWriteValue(atomicString);
        setLeader(null);
        assertThatThrownBy(atomicString::get).isInstanceOf(ServiceUnavailableException.class);
        setLeader(LOCAL_MEMBER_ID);
        assertCanReadAndWriteValue(atomicString);
    }

    @Test
    public void shouldThrow503WhenAnIoExceptionIsThrown() {
        when(LEADER_INFO.get()).thenReturn(Futures.exceptionalFuture(new ConnectException()));

        assertThatThrownBy(atomicString::get)
                .isInstanceOf(ServiceUnavailableException.class);
    }

    @Test
    public void shouldThrowTheUncheckedExecutionExceptionWhenNotIoException() {
        Exception expectedInnerException = new Exception("the inner exception");
        when(LEADER_INFO.get()).thenReturn(Futures.exceptionalFuture(expectedInnerException));

        assertThatThrownBy(atomicString::get)
                .isInstanceOf(UncheckedExecutionException.class)
                .hasCause(expectedInnerException);
    }

    @Test
    public void shouldResetDelegateOnLeaderChange() {
        setLeader(LOCAL_MEMBER_ID);
        atomicString.set(TEST_VALUE);

        setLeader(null);
        assertThatThrownBy(atomicString::get).isInstanceOf(ServiceUnavailableException.class);

        setLeader(LOCAL_MEMBER_ID);
        assertThat(atomicString.get()).isNotEqualTo(TEST_VALUE);
    }

    @Test
    public void shouldCloseDelegateIfCloseableAndLeaderChanges() throws IOException {
        ByteChannel mockedResource = mock(ByteChannel.class);
        ByteChannel proxiedResource = InvalidatingLeaderProxy.create(
                LOCAL_MEMBER,
                LEADER_INFO,
                () -> mockedResource,
                ByteChannel.class);

        setLeader(LOCAL_MEMBER_ID);
        proxiedResource.isOpen();

        setLeader(null);
        assertThatThrownBy(proxiedResource::isOpen).isInstanceOf(ServiceUnavailableException.class);

        verify(mockedResource, times(1)).close();
    }

    @Test
    public void shouldThrowExceptionsThrownFromClosingTheDelegate() throws IOException {
        ByteChannel mockedResource = mock(ByteChannel.class);
        ByteChannel proxiedResource = InvalidatingLeaderProxy.create(
                LOCAL_MEMBER,
                LEADER_INFO,
                () -> mockedResource,
                ByteChannel.class);

        IOException expectedInnerException = new IOException("the inner exception");
        doThrow(expectedInnerException).when(mockedResource).close();

        setLeader(LOCAL_MEMBER_ID);
        proxiedResource.isOpen();

        setLeader(null);
        assertThatThrownBy(proxiedResource::isOpen)
                .isInstanceOf(RuntimeException.class)
                .hasCause(expectedInnerException);
    }

    @Test
    public void shouldResetDelegateWhenTermChanges() {
        setLeader(LOCAL_MEMBER_ID);
        atomicString.set(TEST_VALUE);

        setLeader(LOCAL_MEMBER_ID);
        assertThat(atomicString.get()).isNotEqualTo(TEST_VALUE);
    }

    private void assertCanReadAndWriteValue(AtomicString container) {
        setLeader(LOCAL_MEMBER_ID);
        container.set(TEST_VALUE);
        assertThat(container.get()).isEqualTo(TEST_VALUE);
    }

    @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION") // https://github.com/findbugsproject/findbugs/issues/79
    private void setLeader(@Nullable String newLeader) {
        long newTerm = election.term().term() + 1;
        if (newLeader != null) {
            LeaderAndTerm newLeaderInfo = ImmutableLeaderAndTerm.of(newTerm, newLeader);
            when(LEADER_INFO.get()).thenReturn(CompletableFuture.completedFuture(newLeaderInfo));
        } else {
            when(LEADER_INFO.get()).thenReturn(CompletableFuture.completedFuture(null));
        }

        GroupMember member = mock(GroupMember.class);
        when(member.id()).thenReturn(newLeader);

        election.onTerm(newTerm);
        election.onElection(member);
    }

    private interface AtomicString {
        String get();
        void set(String string);
    }

    private static class SimpleAtomicString implements AtomicString {
        private String string = "";

        @Override
        public String get() {
            return string;
        }

        @Override
        public void set(String newString) {
            this.string = newString;
        }
    }
}
