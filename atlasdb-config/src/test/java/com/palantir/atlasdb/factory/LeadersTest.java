/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.factory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.config.RemotingClientConfig;
import com.palantir.atlasdb.config.RemotingClientConfigs;
import com.palantir.atlasdb.http.AtlasDbRemotingConstants;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosValue;

public class LeadersTest {

    private static final Set<String> REMOTE_SERVICE_ADDRESSES = ImmutableSet.of("foo:1234", "bar:5678");
    private static final Supplier<RemotingClientConfig> REMOTING_CLIENT_CONFIG
            = () -> RemotingClientConfigs.ALWAYS_USE_CONJURE;

    @Test
    public void canCreateProxyAndLocalListOfPaxosLearners() {
        PaxosLearner localLearner = mock(PaxosLearner.class);
        Optional<PaxosValue> presentPaxosValue = Optional.of(mock(PaxosValue.class));
        when(localLearner.getGreatestLearnedValue()).thenReturn(presentPaxosValue);

        List<PaxosLearner> paxosLearners = Leaders.createProxyAndLocalList(
                MetricsManagers.createForTests(),
                localLearner,
                REMOTE_SERVICE_ADDRESSES,
                REMOTING_CLIENT_CONFIG,
                Optional.empty(),
                PaxosLearner.class,
                AtlasDbRemotingConstants.DEFAULT_USER_AGENT);

        assertThat(paxosLearners.size()).isEqualTo(REMOTE_SERVICE_ADDRESSES.size() + 1);
        paxosLearners.forEach(object -> assertThat(object).isNotNull());
        assertThat(Iterables.getLast(paxosLearners).getGreatestLearnedValue()).isEqualTo(value);
        verify(localLearner).getGreatestLearnedValue();
        verifyNoMoreInteractions(localLearner);
    }

    @Test
    public void canCreateProxyAndLocalListOfPaxosAcceptors() {
        PaxosAcceptor localAcceptor = mock(PaxosAcceptor.class);
        when(localAcceptor.getLatestSequencePreparedOrAccepted()).thenReturn(1L);

        List<PaxosAcceptor> paxosAcceptors = Leaders.createProxyAndLocalList(
                MetricsManagers.createForTests(),
                localAcceptor,
                REMOTE_SERVICE_ADDRESSES,
                REMOTING_CLIENT_CONFIG,
                Optional.empty(),
                PaxosAcceptor.class,
                AtlasDbRemotingConstants.DEFAULT_USER_AGENT);

        assertThat(paxosAcceptors.size()).isEqualTo(REMOTE_SERVICE_ADDRESSES.size() + 1);
        paxosAcceptors.forEach(object -> assertThat(object).isNotNull());

        assertThat(Iterables.getLast(paxosAcceptors).getLatestSequencePreparedOrAccepted()).isEqualTo(1L);
        verify(localAcceptor).getLatestSequencePreparedOrAccepted();
        verifyNoMoreInteractions(localAcceptor);
    }

    @Test
    public void createProxyAndLocalListCreatesSingletonListIfNoRemoteAddressesProvided() {
        PaxosAcceptor localAcceptor = mock(PaxosAcceptor.class);
        when(localAcceptor.getLatestSequencePreparedOrAccepted()).thenReturn(1L);

        List<PaxosAcceptor> paxosAcceptors = Leaders.createProxyAndLocalList(
                MetricsManagers.createForTests(),
                localAcceptor,
                ImmutableSet.of(),
                REMOTING_CLIENT_CONFIG,
                Optional.empty(),
                PaxosAcceptor.class,
                AtlasDbRemotingConstants.DEFAULT_USER_AGENT);

        assertThat(paxosAcceptors.size()).isEqualTo(1);

        assertThat(Iterables.getLast(paxosAcceptors).getLatestSequencePreparedOrAccepted()).isEqualTo(1L);
        verify(localAcceptor).getLatestSequencePreparedOrAccepted();
        verifyNoMoreInteractions(localAcceptor);
    }

    @Test(expected = IllegalStateException.class)
    public void createProxyAndLocalListThrowsIfCreatingObjectsWithoutHttpMethodAnnotatedMethods() {
        BigInteger localBigInteger = new BigInteger("0");

        Leaders.createProxyAndLocalList(
                MetricsManagers.createForTests(),
                localBigInteger,
                REMOTE_SERVICE_ADDRESSES,
                REMOTING_CLIENT_CONFIG,
                Optional.empty(),
                BigInteger.class,
                AtlasDbRemotingConstants.DEFAULT_USER_AGENT);
    }

    @Test(expected = NullPointerException.class)
    public void createProxyAndLocalListThrowsIfNullClassProvided() {
        PaxosAcceptor localAcceptor = mock(PaxosAcceptor.class);

        Leaders.createProxyAndLocalList(
                MetricsManagers.createForTests(),
                localAcceptor,
                REMOTE_SERVICE_ADDRESSES,
                REMOTING_CLIENT_CONFIG,
                Optional.empty(),
                null,
                AtlasDbRemotingConstants.DEFAULT_USER_AGENT);
    }
}
