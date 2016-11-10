/**
 * Copyright 2016 Palantir Technologies
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

import javax.ws.rs.ServiceUnavailableException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.Futures;

import io.atomix.AtomixReplica;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.transport.local.LocalServerRegistry;
import io.atomix.catalyst.transport.local.LocalTransport;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.group.DistributedGroup;
import io.atomix.group.LocalMember;
import io.atomix.variables.DistributedValue;

public class InvalidatingLeaderProxyTest {
    private static final Address LOCAL_ADDRESS = new Address("localhost", 8700);

    private static final Transport transport = new LocalTransport(new LocalServerRegistry());
    private static final AtomixReplica replica = AtomixReplica.builder(LOCAL_ADDRESS)
            .withStorage(Storage.builder()
                    .withStorageLevel(StorageLevel.MEMORY)
                    .build())
            .withTransport(transport)
            .build();

    private static final String GROUP_KEY = "groupKey";
    private static final String LEADER_KEY = "groupKey/leader";
    private static final String TEST_VALUE = "testValue";

    private static LocalMember localMember;
    private static DistributedValue<String> leaderId;

    private StringContainer stringContainer;

    @BeforeClass
    public static void startAtomix() {
        replica.bootstrap().join();
        leaderId = Futures.getUnchecked(replica.<String>getValue(LEADER_KEY));
        DistributedGroup distributedGroup = Futures.getUnchecked(replica.getGroup(GROUP_KEY));
        distributedGroup.election().onElection(
                term -> Futures.getUnchecked(leaderId.set(term.leader().id()))
        );
        localMember = Futures.getUnchecked(distributedGroup.join());
    }

    @AfterClass
    public static void stopAtomix() {
        replica.leave();
    }

    @Before
    public void setUp() {
        stringContainer = InvalidatingLeaderProxy.create(
                localMember,
                leaderId,
                StringContainerImpl::new,
                StringContainer.class);
    }

    @Test
    public void shouldCallDelegateIfLeader() {
        assertCanReadAndWriteValue(stringContainer);
    }

    @Test
    public void shouldBeUnableToCallDelegateIfNotLeader() {
        String oldLeader = Futures.getUnchecked(leaderId.get());
        try {
            setLeader(null);
            assertThatThrownBy(stringContainer::get).isInstanceOf(ServiceUnavailableException.class);
        } finally {
            setLeader(oldLeader);
        }
    }

    @Test
    public void shouldBeAbleToCallDelegateAgainOnRegainingLeadership() {
        String oldLeader = Futures.getUnchecked(leaderId.get());
        try {
            assertCanReadAndWriteValue(stringContainer);
            setLeader(null);
            assertThatThrownBy(stringContainer::get).isInstanceOf(ServiceUnavailableException.class);
            setLeader(oldLeader);
            assertCanReadAndWriteValue(stringContainer);
        } finally {
            setLeader(oldLeader);
        }
    }

    private void setLeader(String leader) {
        Futures.getUnchecked(leaderId.set(leader));
    }

    private void assertCanReadAndWriteValue(StringContainer container) {
        container.set(TEST_VALUE);
        assertThat(container.get()).isEqualTo(TEST_VALUE);
    }

    private interface StringContainer {
        String get();
        void set(String string);
    }

    private static class StringContainerImpl implements StringContainer {
        private String string;

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
