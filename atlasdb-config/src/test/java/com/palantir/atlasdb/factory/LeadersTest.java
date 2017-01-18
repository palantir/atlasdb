/**
 * Copyright 2017 Palantir Technologies
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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

import java.math.BigInteger;
import java.util.List;
import java.util.Set;

import org.hamcrest.MatcherAssert;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;

public class LeadersTest {

    public static final Set<String> REMOTE_SERVICE_ADDRESSES = ImmutableSet.of("foo:1234", "bar:5678");

    @Test
    public void canCreateProxyAndLocalListOfPaxosLearners() {
        PaxosLearner localLearner = mock(PaxosLearner.class);

        List<PaxosLearner> paxosLearners = Leaders.createProxyAndLocalList(
                localLearner,
                REMOTE_SERVICE_ADDRESSES,
                Optional.absent(),
                PaxosLearner.class);

        MatcherAssert.assertThat(paxosLearners.size(), is(REMOTE_SERVICE_ADDRESSES.size() + 1));
        MatcherAssert.assertThat(paxosLearners.contains(localLearner), is(true));
        paxosLearners.forEach(object -> MatcherAssert.assertThat(object, not(nullValue())));
    }

    @Test
    public void canCreateProxyAndLocalListOfPaxosAcceptors() {
        PaxosAcceptor localAcceptor = mock(PaxosAcceptor.class);

        List<PaxosAcceptor> paxosAcceptors = Leaders.createProxyAndLocalList(
                localAcceptor,
                REMOTE_SERVICE_ADDRESSES,
                Optional.absent(),
                PaxosAcceptor.class);

        MatcherAssert.assertThat(paxosAcceptors.size(), is(REMOTE_SERVICE_ADDRESSES.size() + 1));
        MatcherAssert.assertThat(paxosAcceptors.contains(localAcceptor), is(true));
        paxosAcceptors.forEach(object -> MatcherAssert.assertThat(object, not(nullValue())));
    }

    @Test
    public void createProxyAndLocalListCreatesSingletonListIfNoRemoteAddressesProvided() {
        PaxosAcceptor localAcceptor = mock(PaxosAcceptor.class);

        List<PaxosAcceptor> paxosAcceptors = Leaders.createProxyAndLocalList(
                localAcceptor,
                ImmutableSet.of(),
                Optional.absent(),
                PaxosAcceptor.class);

        MatcherAssert.assertThat(paxosAcceptors, contains(localAcceptor));
    }

    @Test(expected = IllegalStateException.class)
    public void createProxyAndLocalListThrowsIfCreatingObjectsWithoutHttpMethodAnnotatedMethods() {
        BigInteger localBigInteger = new BigInteger("0");

        Leaders.createProxyAndLocalList(
                localBigInteger,
                REMOTE_SERVICE_ADDRESSES,
                Optional.absent(),
                BigInteger.class);
    }

    @Test(expected = NullPointerException.class)
    public void createProxyAndLocalListThrowsIfNullClassProvided() {
        PaxosAcceptor localAcceptor = mock(PaxosAcceptor.class);

        Leaders.createProxyAndLocalList(
                localAcceptor,
                REMOTE_SERVICE_ADDRESSES,
                Optional.absent(),
                null);
    }
}
