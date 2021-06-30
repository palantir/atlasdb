/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.config;

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public enum PermittedKeyValueServiceTypes {
    /*
     * "relational" is hard-coded from DbKeyValueServiceConfig
     * to avoid taking a compile time dependency on atlasdb-dbkvs
     */
    RELATIONAL("relational"),
    MEMORY("memory");

    private static final Set<String> PERMITTED_TYPES = Arrays.stream(PermittedKeyValueServiceTypes.values())
            .map(specificType -> specificType.type)
            .collect(Collectors.toSet());

    private final String type;

    PermittedKeyValueServiceTypes(String type) {
        this.type = type;
    }

    static void checkKeyValueServiceTypeIsPermitted(String typeIdentifier) {
        Preconditions.checkArgument(
                PERMITTED_TYPES.contains(typeIdentifier),
                "Only InMemory/Dbkvs is a supported for TimeLock's database persister. Found %s.",
                SafeArg.of("userTypeIdentifier", typeIdentifier));
    }
}
