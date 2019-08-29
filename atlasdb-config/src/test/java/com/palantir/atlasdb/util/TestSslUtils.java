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

package com.palantir.atlasdb.util;

import java.nio.file.Paths;

import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.conjure.java.config.ssl.TrustContext;

public final class TestSslUtils {
    public static final TrustContext TRUST_CONTEXT =
            SslSocketFactories.createTrustContext(SslConfiguration.of(Paths.get("var/security/trustStore.jks")));

    public static final SslConfiguration SSL = SslConfiguration.of(
            Paths.get("var/security/trustStore.jks"),
            Paths.get("var/security/keyStore.jks"),
            "keystore");

    private TestSslUtils() {
        // utility
    }
}
