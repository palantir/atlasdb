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

import com.palantir.common.streams.KeyedStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

public enum AtlasDbHttpProtocolVersion {
    LEGACY_OR_UNKNOWN("1.0"),
    CONJURE_JAVA_RUNTIME("2.0");

    private static final Map<String, AtlasDbHttpProtocolVersion> KNOWN_VERSION_STRINGS
            = KeyedStream.of(Arrays.stream(AtlasDbHttpProtocolVersion.values()))
                    .mapKeys(AtlasDbHttpProtocolVersion::getProtocolVersionString)
                    .collectToMap();

    private final String protocolVersionString;

    AtlasDbHttpProtocolVersion(String protocolVersionString) {
        this.protocolVersionString = protocolVersionString;
    }

    public String getProtocolVersionString() {
        return protocolVersionString;
    }

    public static AtlasDbHttpProtocolVersion inferFromString(Optional<String> protocolVersion) {
        return protocolVersion.map(KNOWN_VERSION_STRINGS::get).orElse(LEGACY_OR_UNKNOWN);
    }
}
