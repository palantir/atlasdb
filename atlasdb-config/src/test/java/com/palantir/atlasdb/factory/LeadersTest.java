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
package com.palantir.atlasdb.factory;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.swing.text.html.Option;

import org.hamcrest.MatcherAssert;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.util.MetricsRule;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosValue;

public class LeadersTest {

    public static final Set<String> REMOTE_SERVICE_ADDRESSES = ImmutableSet.of("foo:1234", "bar:5678");

    @Rule
    public MetricsRule metricsRule = new MetricsRule();

    @Test
    public void canCreateProxyAndLocalListOfPaxosLearners() {
        PaxosLearner localLearner = mock(PaxosLearner.class);
        PaxosValue value = mock(PaxosValue.class);
        when(localLearner.getGreatestLearnedValue()).thenReturn(value);

        List<PaxosLearner> paxosLearners = Leaders.createProxyAndLocalList(
                localLearner,
                REMOTE_SERVICE_ADDRESSES,
                Optional.empty(),
                PaxosLearner.class);

        MatcherAssert.assertThat(paxosLearners.size(), is(REMOTE_SERVICE_ADDRESSES.size() + 1));
        paxosLearners.forEach(object -> MatcherAssert.assertThat(object, not(nullValue())));
        MatcherAssert.assertThat(Iterables.getLast(paxosLearners).getGreatestLearnedValue(), is(value));
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
                Optional.empty(),
                PaxosAcceptor.class);

        MatcherAssert.assertThat(paxosAcceptors.size(), is(REMOTE_SERVICE_ADDRESSES.size() + 1));
        paxosAcceptors.forEach(object -> MatcherAssert.assertThat(object, not(nullValue())));

        MatcherAssert.assertThat(Iterables.getLast(paxosAcceptors).getLatestSequencePreparedOrAccepted(), is(1L));
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
                Optional.empty(),
                PaxosAcceptor.class);

        MatcherAssert.assertThat(paxosAcceptors.size(), is(1));

        MatcherAssert.assertThat(Iterables.getLast(paxosAcceptors).getLatestSequencePreparedOrAccepted(), is(1L));
        verify(localAcceptor).getLatestSequencePreparedOrAccepted();
        verifyNoMoreInteractions(localAcceptor);
    }

    @Test(expected = IllegalStateException.class)
    public void createProxyAndLocalListThrowsIfCreatingObjectsWithoutHttpMethodAnnotatedMethods() {
        BigInteger localBigInteger = new BigInteger("0");

        Leaders.createProxyAndLocalList(
                localBigInteger,
                REMOTE_SERVICE_ADDRESSES,
                Optional.empty(),
                BigInteger.class);
    }

    @Test(expected = NullPointerException.class)
    public void createProxyAndLocalListThrowsIfNullClassProvided() {
        PaxosAcceptor localAcceptor = mock(PaxosAcceptor.class);

        Leaders.createProxyAndLocalList(
                localAcceptor,
                REMOTE_SERVICE_ADDRESSES,
                Optional.empty(),
                null);
    }
}
