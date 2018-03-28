/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.ws.rs.NotAuthorizedException;

import org.junit.Before;
import org.junit.Test;

import com.palantir.tokens.auth.AuthHeader;

public class SecureTimelockServiceImplTest {

    private SecureTimelockServiceImpl secureTimelockService;
    private AsyncTimelockService mockService = mock(AsyncTimelockService.class);

    @Before
    public void setUp() {
        AuthHeader clientAuthHeader = AuthHeader.valueOf("foo");
        secureTimelockService = new SecureTimelockServiceImpl(mockService, clientAuthHeader);
    }

    @Test(expected = NotAuthorizedException.class)
    public void getFreshTimestampFailsIfTokensDontMatch() {
        secureTimelockService.getFreshTimestamp(AuthHeader.valueOf("bar"));
    }

    @Test
    public void canGetFreshTimestampIfTokensMatch() {
        when(mockService.getFreshTimestamp()).thenReturn(5L);

        assertEquals(5L, secureTimelockService.getFreshTimestamp(AuthHeader.valueOf("foo")));
    }

    @Test(expected = NotAuthorizedException.class)
    public void insecureFreshTimestampMethodFails() {
        when(mockService.getFreshTimestamp()).thenReturn(5L);

        secureTimelockService.getFreshTimestamp();
    }

}
