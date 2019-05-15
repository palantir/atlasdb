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

package com.palantir.atlasdb.timelock.auth.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import javax.ws.rs.ForbiddenException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.timelock.auth.api.Authenticator;
import com.palantir.atlasdb.timelock.auth.api.BCryptedSecret;
import com.palantir.atlasdb.timelock.auth.api.Client;
import com.palantir.atlasdb.timelock.auth.api.Password;

@RunWith(MockitoJUnitRunner.class)
public class CachingAuthenticatorTest {

    private static final String CLIENT_1 = "client_1";
    private static final String CLIENT_2 = "client_2";

    private static final Password PASSWORD_1 = Password.of("password_1");
    private static final Password PASSWORD_2 = Password.of("password_2");

    @Test
    public void returnsAnonymousClientIfUnaware() {
        Authenticator cachingAuthenticator = CachingAuthenticator.create(ImmutableMap.of());
        assertThat(cachingAuthenticator.authenticate(CLIENT_1, PASSWORD_1)).isEqualTo(Client.ANONYMOUS);
    }

    @Test
    public void authenticatesClientIfExistsInCredentials() {
        Authenticator cachingAuthenticator = CachingAuthenticator.create(ImmutableMap.of(
                CLIENT_1, bcrypted(PASSWORD_1),
                CLIENT_2, bcrypted(PASSWORD_2)));

        assertThat(cachingAuthenticator.authenticate(CLIENT_1, PASSWORD_1)).
                isEqualTo(Client.create(CLIENT_1));
    }

    @Test
    public void throwsForbiddenExceptionIfPasswordDoesNotMatch() {
        Authenticator cachingAuthenticator = CachingAuthenticator.create(ImmutableMap.of(
                CLIENT_1, bcrypted(PASSWORD_1),
                CLIENT_2, bcrypted(PASSWORD_2)));

        assertThatThrownBy(() -> cachingAuthenticator.authenticate(CLIENT_1, PASSWORD_2))
                .isInstanceOf(ForbiddenException.class);
    }

    private static BCryptedSecret bcrypted(Password password) {
        return BCryptedSecret.forPassword(password);
    }
}