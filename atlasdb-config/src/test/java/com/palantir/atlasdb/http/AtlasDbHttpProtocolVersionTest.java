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

package com.palantir.atlasdb.http;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.stream.Stream;
import org.junit.Test;

public class AtlasDbHttpProtocolVersionTest {
    @Test
    public void infersFromKnownVersionStrings() {
        Stream.of(AtlasDbHttpProtocolVersion.LEGACY_OR_UNKNOWN, AtlasDbHttpProtocolVersion.CONJURE_JAVA_RUNTIME)
                .forEach(value -> {
                    Optional<String> protocolVersionString = Optional.of(value.getProtocolVersionString());
                    assertThat(AtlasDbHttpProtocolVersion.inferFromString(protocolVersionString))
                            .isEqualTo(value);
                });
    }

    @Test
    public void infersLegacyIfNoVersionStringProvided() {
        assertThat(AtlasDbHttpProtocolVersion.inferFromString(Optional.empty()))
                .isEqualTo(AtlasDbHttpProtocolVersion.LEGACY_OR_UNKNOWN);
    }

    @Test
    public void infersLegacyIfVersionStringIsUnintelligible() {
        assertThat(AtlasDbHttpProtocolVersion.inferFromString(Optional.of("this is a bad version string!111!!!")))
                .isEqualTo(AtlasDbHttpProtocolVersion.LEGACY_OR_UNKNOWN);
    }
}
