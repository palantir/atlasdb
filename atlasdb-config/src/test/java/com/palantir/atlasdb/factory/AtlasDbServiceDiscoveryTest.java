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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.namespacedeleter.NamespaceDeleterFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfigHelper;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.refreshable.Refreshable;
import java.util.Optional;
import org.junit.Test;

public final class AtlasDbServiceDiscoveryTest {
    private static final KeyValueServiceConfig INVALID_KVS_CONFIG = new TestKeyValueServiceConfig(true, "fakeconfig");
    private static final Refreshable<Optional<KeyValueServiceRuntimeConfig>> RUNTIME_CONFIG =
            Refreshable.only(Optional.empty());
    private final NamespaceDeleterFactory delegate = new AutoServiceAnnotatedNamespaceDeleterFactory();

    @Test
    public void createsNamespaceDeleterFactoriesAnnotatedWithAutoService() {
        KeyValueServiceConfig deletionEnabledConfig =
                new TestKeyValueServiceConfig(true, AutoServiceAnnotatedNamespaceDeleterFactory.TYPE);
        NamespaceDeleterFactory namespaceDeleterFactory = createNamespaceDeleterFactory(deletionEnabledConfig);

        assertThat(namespaceDeleterFactory).isInstanceOf(AutoServiceAnnotatedNamespaceDeleterFactory.class);
        assertThat(namespaceDeleterFactory.createNamespaceDeleter(deletionEnabledConfig, RUNTIME_CONFIG))
                .isEqualTo(delegate.createNamespaceDeleter(deletionEnabledConfig, RUNTIME_CONFIG));
    }

    @Test
    public void creatingNamespaceDeleterFactoryThrowsIfDeletionDisabled() {
        KeyValueServiceConfig deletionEnabledConfig =
                new TestKeyValueServiceConfig(false, AutoServiceAnnotatedNamespaceDeleterFactory.TYPE);

        assertThatThrownBy(() -> createNamespaceDeleterFactory(deletionEnabledConfig))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessage("Cannot construct a NamespaceDeleterFactory when"
                        + " keyValueService.enableNamespaceDeletionDangerousIKnowWhatIAmDoing is false.");
    }

    @Test
    public void notAllowConstructionWithoutAValidBackingNamespaceDeleterFactory() {
        assertThatIllegalStateException()
                .isThrownBy(() -> createNamespaceDeleterFactory(INVALID_KVS_CONFIG))
                .withMessageContaining("No atlas provider")
                .withMessageContaining(INVALID_KVS_CONFIG.type());
    }

    private static NamespaceDeleterFactory createNamespaceDeleterFactory(KeyValueServiceConfig config) {
        return AtlasDbServiceDiscovery.createNamespaceDeleterFactoryOfCorrectType(config);
    }

    private static final class TestKeyValueServiceConfig implements KeyValueServiceConfigHelper {
        private final boolean enableDeletion;
        private final String type;

        private TestKeyValueServiceConfig(boolean enableDeletion, String type) {
            this.enableDeletion = enableDeletion;
            this.type = type;
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public boolean enableNamespaceDeletion() {
            return enableDeletion;
        }
    }
}
