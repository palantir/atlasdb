/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import static com.palantir.conjure.java.api.testing.Assertions.assertThatServiceExceptionThrownBy;
import static org.mockito.Mockito.when;

import com.palantir.logsafe.SafeArg;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BatchPaxosAcceptorResourceTests {

    @Mock private BatchPaxosAcceptor acceptorDelegate;

    private BatchPaxosAcceptorResource resource;

    @Before
    public void setUp() {
        resource = new BatchPaxosAcceptorResource(acceptorDelegate);
    }

    @Test
    public void throwsConjureRuntime404WhenCacheKeyIsNotFound() throws InvalidAcceptorCacheKeyException {
        AcceptorCacheKey cacheKey = AcceptorCacheKey.newCacheKey();
        when(acceptorDelegate.latestSequencesPreparedOrAcceptedCached(cacheKey))
                .thenThrow(new InvalidAcceptorCacheKeyException(cacheKey));

        assertThatServiceExceptionThrownBy(() ->
                resource.latestSequencesPreparedOrAcceptedCached(Optional.of(cacheKey)))
                .hasType(BatchPaxosAcceptorRpcClient.CACHE_KEY_NOT_FOUND)
                .hasArgs(SafeArg.of("cacheKey", cacheKey));
    }

}
