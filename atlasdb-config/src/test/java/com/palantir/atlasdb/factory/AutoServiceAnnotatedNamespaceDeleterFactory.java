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
package com.palantir.atlasdb.factory;

import static org.mockito.Mockito.mock;

import com.google.auto.service.AutoService;
import com.palantir.atlasdb.namespacedeleter.NamespaceDeleter;
import com.palantir.atlasdb.namespacedeleter.NamespaceDeleterFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.refreshable.Refreshable;
import java.util.Optional;

@AutoService(NamespaceDeleterFactory.class)
public class AutoServiceAnnotatedNamespaceDeleterFactory implements NamespaceDeleterFactory {
    public static final String TYPE = "not-a-real-db";
    private static final NamespaceDeleter NAMESPACE_DELETER = mock(NamespaceDeleter.class);

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public NamespaceDeleter createNamespaceDeleter(
            KeyValueServiceConfig config, Refreshable<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig) {
        return NAMESPACE_DELETER;
    }
}
