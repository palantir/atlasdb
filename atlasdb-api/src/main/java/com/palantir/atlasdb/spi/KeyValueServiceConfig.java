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
package com.palantir.atlasdb.spi;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Marker interface for various AtlasDb KeyValueService config objects.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = As.PROPERTY, property = "type", visible = false)
public interface KeyValueServiceConfig {
    String type();

    Optional<String> namespace();

    Optional<SharedResourcesConfig> sharedResourcesConfig();

    /**
     * Enables construction of {@link com.palantir.atlasdb.namespacedeleter.NamespaceDeleter} via a
     * {@link com.palantir.atlasdb.namespacedeleter.NamespaceDeleterFactory} through AtlasDbServiceDiscovery, which
     * can be used to delete all data for a namespace in the KVS. This is dangerous, and must only be used once
     * you've acknowledged the risks and side effects mentioned in the relevant NamespaceDeleter docs (e.g.,
     * CassandraNamespaceDeleter)
     */
    @Value.Default // Without this annotation, implementors cannot add this property as part of an immutable builder.
    default boolean enableNamespaceDeletionDangerousIKnowWhatIAmDoing() {
        return false;
    }
}
