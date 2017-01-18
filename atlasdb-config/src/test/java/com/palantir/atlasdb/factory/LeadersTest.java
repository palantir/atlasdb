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

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Set;

import org.hamcrest.MatcherAssert;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.palantir.paxos.PaxosLearner;

public class LeadersTest {
    @Test
    public void canCreateProxyAndLocalList() {
        Set<String> remoteServices = ImmutableSet.of("foo:1234", "bar:5678");
        PaxosLearner localService = mock(PaxosLearner.class);

        List<PaxosLearner> proxyAndLocalList = Leaders.createProxyAndLocalList(localService,
                remoteServices,
                Optional.absent(),
                PaxosLearner.class);
        MatcherAssert.assertThat(proxyAndLocalList.size(), is(remoteServices.size() + 1));
        MatcherAssert.assertThat(proxyAndLocalList.contains(localService), is(true));
    }
}
