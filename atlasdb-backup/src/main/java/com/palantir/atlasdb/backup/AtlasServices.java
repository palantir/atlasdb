/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.backup;

import com.palantir.atlasdb.backup.api.AtlasService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public final class AtlasServices {
    private AtlasServices() {
        // util
    }

    public static void throwIfAtlasServicesCollide(Set<AtlasService> atlasServices) {
        Map<Namespace, Long> namespacesByCount = atlasServices.stream()
                .collect(Collectors.groupingBy(AtlasService::getNamespace, Collectors.counting()));
        Set<Namespace> duplicatedNamespaces = namespacesByCount.entrySet().stream()
                .filter(entryWithCount -> entryWithCount.getValue() > 1)
                .map(Entry::getKey)
                .collect(Collectors.toSet());
        if (!duplicatedNamespaces.isEmpty()) {
            throw new SafeIllegalArgumentException(
                    "Duplicated namespaces found in request. The operation cannot safely proceed.",
                    SafeArg.of("duplicatedNamespaces", duplicatedNamespaces),
                    SafeArg.of("atlasServices", atlasServices));
        }
    }
}
