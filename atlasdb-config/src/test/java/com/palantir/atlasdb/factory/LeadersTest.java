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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.config.RemotingClientConfig;
import com.palantir.atlasdb.config.RemotingClientConfigs;
import com.palantir.atlasdb.http.AtlasDbRemotingConstants;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosValue;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.junit.Test;

public class LeadersTest {

    private static final SslConfiguration SSL_CONFIGURATION =
            SslConfiguration.of(Paths.get("var/security/trustStore.jks"));
    private static final TrustContext TRUST_CONTEXT = SslSocketFactories.createTrustContext(SSL_CONFIGURATION);
    private static final ImmutableSet<String> REMOTE_SERVICE_ADDRESSES =
            ImmutableSet.of("https://foo:1234", "https://bar:5678");
    private static final Supplier<RemotingClientConfig> REMOTING_CLIENT_CONFIG = () -> RemotingClientConfigs.DEFAULT;

    @Test
    public void canCreateProxyAndLocalListOfPaxosLearners() {
        PaxosLearner localLearner = mock(PaxosLearner.class);
        Optional<PaxosValue> presentPaxosValue = Optional.of(mock(PaxosValue.class));
        when(localLearner.getGreatestLearnedValue()).thenReturn(presentPaxosValue);

        List<PaxosLearner> paxosLearners = Leaders.createProxyAndLocalList(
                localLearner,
                REMOTE_SERVICE_ADDRESSES,
                REMOTING_CLIENT_CONFIG,
                Optional.of(TRUST_CONTEXT),
                PaxosLearner.class,
                AtlasDbRemotingConstants.DEFAULT_USER_AGENT);

        assertThat(paxosLearners).hasSize(REMOTE_SERVICE_ADDRESSES.size() + 1);
        paxosLearners.forEach(object -> assertThat(object).isNotNull());
        assertThat(Iterables.getLast(paxosLearners).getGreatestLearnedValue()).isEqualTo(presentPaxosValue);
        verify(localLearner).getGreatestLearnedValue();
        verifyNoMoreInteractions(localLearner);
    }

    @Test
    public void canCreateProxyAndLocalListOfPaxosAcceptors() {
        PaxosAcceptor localAcceptor = mock(PaxosAcceptor.class);
        when(localAcceptor.getLatestSequencePreparedOrAccepted()).thenReturn(1L);

        List<PaxosAcceptor> paxosAcceptors = Leaders.createProxyAndLocalList(
                localAcceptor,
                REMOTE_SERVICE_ADDRESSES,
                REMOTING_CLIENT_CONFIG,
                Optional.of(TRUST_CONTEXT),
                PaxosAcceptor.class,
                AtlasDbRemotingConstants.DEFAULT_USER_AGENT);

        assertThat(paxosAcceptors).hasSize(REMOTE_SERVICE_ADDRESSES.size() + 1);
        paxosAcceptors.forEach(object -> assertThat(object).isNotNull());

        assertThat(Iterables.getLast(paxosAcceptors).getLatestSequencePreparedOrAccepted())
                .isEqualTo(1L);
        verify(localAcceptor).getLatestSequencePreparedOrAccepted();
        verifyNoMoreInteractions(localAcceptor);
    }

    @Test
    public void createProxyAndLocalListCreatesSingletonListIfNoRemoteAddressesProvided() {
        PaxosAcceptor localAcceptor = mock(PaxosAcceptor.class);
        when(localAcceptor.getLatestSequencePreparedOrAccepted()).thenReturn(1L);

        List<PaxosAcceptor> paxosAcceptors = Leaders.createProxyAndLocalList(
                localAcceptor,
                ImmutableSet.of(),
                REMOTING_CLIENT_CONFIG,
                Optional.of(TRUST_CONTEXT),
                PaxosAcceptor.class,
                AtlasDbRemotingConstants.DEFAULT_USER_AGENT);

        assertThat(paxosAcceptors).hasSize(1);

        assertThat(Iterables.getLast(paxosAcceptors).getLatestSequencePreparedOrAccepted())
                .isEqualTo(1L);
        verify(localAcceptor).getLatestSequencePreparedOrAccepted();
        verifyNoMoreInteractions(localAcceptor);
    }

    @Test(expected = NullPointerException.class)
    public void createProxyAndLocalListThrowsIfNullClassProvided() {
        PaxosAcceptor localAcceptor = mock(PaxosAcceptor.class);

        Leaders.createProxyAndLocalList(
                localAcceptor,
                REMOTE_SERVICE_ADDRESSES,
                REMOTING_CLIENT_CONFIG,
                Optional.of(TRUST_CONTEXT),
                null,
                AtlasDbRemotingConstants.DEFAULT_USER_AGENT);
    }
}
