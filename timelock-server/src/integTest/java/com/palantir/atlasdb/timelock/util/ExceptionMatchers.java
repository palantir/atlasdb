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
package com.palantir.atlasdb.timelock.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.http.AtlasDbHttpProtocolVersion;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public final class ExceptionMatchers {

    private ExceptionMatchers() { }

    public static void assertIndicativeOfNotBeingCurrentLeader(Throwable throwable) {
        assertIndicativeOfNotBeingCurrentLeader(AtlasDbHttpClients.CLIENT_VERSION, throwable);
    }

    public static void assertIndicativeOfNotBeingCurrentLeader(
            AtlasDbHttpProtocolVersion version,
            Throwable throwable) {
        switch (version) {
            case LEGACY_OR_UNKNOWN:
                assertNotCurrentLeaderLegacy(throwable);
                return;
            case CONJURE_JAVA_RUNTIME:
                assertNotCurrentLeaderConjureJavaRuntime(throwable);
                return;
        }
        throw new SafeIllegalStateException("Unknown AtlasDB http protocol version: {}",
                SafeArg.of("version", version));
    }

    private static void assertNotCurrentLeaderConjureJavaRuntime(Throwable throwable) {
        assertThat(throwable).hasMessageContaining("QosException.RetryOther");

        // We shade Feign, so we can't rely on our client's RetryableException exactly matching ours.
        assertThat(throwable.getClass().getName())
                .contains("RetryableException");
    }

    private static void assertNotCurrentLeaderLegacy(Throwable throwable) {
        assertThat(throwable).hasMessageContaining("method invoked on a non-leader");

        // We shade Feign, so we can't rely on our client's RetryableException exactly matching ours.
        assertThat(throwable.getClass().getName())
                .contains("RetryableException");
    }
}
