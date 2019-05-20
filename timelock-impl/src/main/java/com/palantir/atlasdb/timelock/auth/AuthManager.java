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

package com.palantir.atlasdb.timelock.auth;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.ws.rs.ForbiddenException;

import org.immutables.value.Value;

import com.palantir.atlasdb.http.PollingRefreshable;
import com.palantir.atlasdb.timelock.auth.api.AuthenticatedClient;
import com.palantir.atlasdb.timelock.auth.api.Authenticator;
import com.palantir.atlasdb.timelock.auth.api.Authorizer;
import com.palantir.atlasdb.timelock.auth.api.BCryptedSecret;
import com.palantir.atlasdb.timelock.auth.api.Credentials;
import com.palantir.atlasdb.timelock.auth.api.Password;
import com.palantir.atlasdb.timelock.auth.api.Privileges;
import com.palantir.atlasdb.timelock.auth.config.PrivilegesConfiguration;
import com.palantir.atlasdb.timelock.auth.config.TimelockAuthConfiguration;
import com.palantir.atlasdb.timelock.auth.impl.AuthRequirement;
import com.palantir.atlasdb.timelock.auth.impl.CachingAuthenticator;
import com.palantir.atlasdb.timelock.auth.impl.SimpleAuthorizer;
import com.palantir.conjure.java.ext.refresh.Refreshable;
import com.palantir.lock.TimelockNamespace;

public class AuthManager implements AutoCloseable {
    private final Refreshable<TimelockAuthConfiguration> authConfigurationRefreshable;
    private final Runnable closingCallback;
    private volatile AuthServices authServices;

    AuthManager(Refreshable<TimelockAuthConfiguration> authConfigurationRefreshable,
            Authorizer authorizer,
            Authenticator authenticator,
            Runnable closingCallback) {
        this.authConfigurationRefreshable = authConfigurationRefreshable;
        this.closingCallback = closingCallback;
        this.authorizer = authorizer;
        this.authenticator = authenticator;
    }

    public static AuthManager of(Supplier<TimelockAuthConfiguration> timelockAuthConfigurationSupplier) {
        PollingRefreshable<TimelockAuthConfiguration> configPollingRefreshable = PollingRefreshable
                .create(timelockAuthConfigurationSupplier);

        TimelockAuthConfiguration configuration = timelockAuthConfigurationSupplier.get();

        return new AuthManager(
                configPollingRefreshable.getRefreshable(),
                getAuthorizer(configuration),
                getAuthenticator(configuration),
                configPollingRefreshable::close);
    }

    public void checkAuthorized(String clientId, Password password, TimelockNamespace timelockNamespace) {
        update();

        if (!useAuth) {
            return;
        }

        Optional<AuthenticatedClient> client = authenticator.authenticate(clientId, password);
        if (!client.isPresent() || !authorizer.isAuthorized(client.get(), timelockNamespace)) {
            throw new ForbiddenException();
        }
    }

    @Override
    public void close() {
        closingCallback.run();
    }

    private void update() {
        authConfigurationRefreshable.getAndClear().ifPresent(this::updateInternal);
    }

    private synchronized void updateInternal(TimelockAuthConfiguration authConfiguration) {
        authServices = getAuthServices(authConfiguration);
    }

    private static AuthServices getAuthServices(TimelockAuthConfiguration authConfiguration) {
        Authorizer newAuthorizer = getAuthorizer(authConfiguration);
        Authenticator newAuthenticator = getAuthenticator(authConfiguration);

        return ImmutableAuthServices.builder()
                .authenticator(newAuthenticator)
                .authorizer(newAuthorizer)
                .useAuth(authConfiguration.useAuth())
                .build();
    }

    private static Authorizer getAuthorizer(TimelockAuthConfiguration authConfiguration) {
        Map<AuthenticatedClient, Privileges> privilegesMap = authConfiguration.privileges().stream()
                .collect(Collectors.toMap(p -> AuthenticatedClient.create(p.id()), PrivilegesConfiguration::privileges));
        return SimpleAuthorizer.of(privilegesMap, AuthRequirement.PRIVILEGE_BASED);
    }

    private static Authenticator getAuthenticator(TimelockAuthConfiguration authConfiguration) {
        Map<String, BCryptedSecret> secretMap = authConfiguration.credentials().stream()
                .collect(Collectors.toMap(Credentials::id, Credentials::password));
        return CachingAuthenticator.create(secretMap);
    }

    @Value.Immutable
    interface AuthServices {
        Authenticator authenticator();
        Authorizer authorizer();
        boolean useAuth();
    }
}
