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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.timelock.auth.api.AuthenticatedClient;
import com.palantir.atlasdb.timelock.auth.api.Authenticator;
import com.palantir.atlasdb.timelock.auth.api.BCryptedSecret;
import com.palantir.atlasdb.timelock.auth.api.ClientId;
import com.palantir.atlasdb.timelock.auth.api.Password;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

public class CachingAuthenticator implements Authenticator {
    private final ImmutableMap<ClientId, BCryptedSecret> credentials;

    private final LoadingCache<ClientCredentials, Optional<AuthenticatedClient>> cache;

    CachingAuthenticator(Map<ClientId, BCryptedSecret> credentials) {
        this.credentials = ImmutableMap.copyOf(credentials);
        this.cache = Caffeine.newBuilder().maximumSize(1000).build(this::authenticateInternal);
    }

    public static Authenticator create(Map<ClientId, BCryptedSecret> credentials) {
        return new CachingAuthenticator(credentials);
    }

    @Override
    public Optional<AuthenticatedClient> authenticate(ClientId id, Password password) {
        return cache.get(ClientCredentials.of(id, password));
    }

    private Optional<AuthenticatedClient> authenticateInternal(ClientCredentials clientCredentials) {
        BCryptedSecret secret = credentials.get(clientCredentials.id());
        if (secret == null) {
            return Optional.of(AuthenticatedClient.ANONYMOUS);
        }
        return secret.check(clientCredentials.password())
                ? Optional.of(AuthenticatedClient.create(clientCredentials.id()))
                : Optional.empty();
    }

    @Value.Immutable
    interface ClientCredentials {
        ClientId id();

        Password password();

        static ClientCredentials of(ClientId id, Password password) {
            return ImmutableClientCredentials.builder()
                    .id(id)
                    .password(password)
                    .build();
        }
    }
}
