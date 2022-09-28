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
package com.palantir.atlasdb.performance.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import java.net.InetSocketAddress;
import java.util.Optional;
import org.junit.Test;

public class KeyValueServiceInstrumentationTest {

    @Test
    public void forDatabaseAddsInstrumentationFromCorrectClassName() {
        MockKeyValueServiceInstrumentation mockKeyValueServiceInstrumentation =
                new MockKeyValueServiceInstrumentation();

        KeyValueServiceInstrumentation.forDatabase(mockKeyValueServiceInstrumentation.getClassName());

        assertThat(KeyValueServiceInstrumentation.forDatabase(mockKeyValueServiceInstrumentation.getClassName()))
                .isExactlyInstanceOf(MockKeyValueServiceInstrumentation.class);
        assertThat(KeyValueServiceInstrumentation.forDatabase(mockKeyValueServiceInstrumentation.toString()))
                .isExactlyInstanceOf(MockKeyValueServiceInstrumentation.class);

        KeyValueServiceInstrumentation.removeBackendType(mockKeyValueServiceInstrumentation);
    }

    @Test
    public void forDatabaseThrowsForInvalidClassName() {
        assertThatThrownBy(() -> KeyValueServiceInstrumentation.forDatabase("FAKE_BACKEND"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Exception trying to instantiate class FAKE_BACKEND");
    }

    @Test
    public void canAddNewBackendType() {
        DummyKeyValueServiceInstrumentation dummyKeyValueServiceInstrumentation =
                new DummyKeyValueServiceInstrumentation(5, "foo");
        KeyValueServiceInstrumentation.addNewBackendType(dummyKeyValueServiceInstrumentation);
        assertThat(KeyValueServiceInstrumentation.forDatabase(DummyKeyValueServiceInstrumentation.class.getName()))
                .isExactlyInstanceOf(DummyKeyValueServiceInstrumentation.class);
        assertThat(KeyValueServiceInstrumentation.forDatabase(dummyKeyValueServiceInstrumentation.toString()))
                .isExactlyInstanceOf(DummyKeyValueServiceInstrumentation.class);
        KeyValueServiceInstrumentation.removeBackendType(dummyKeyValueServiceInstrumentation);
    }

    private static class DummyKeyValueServiceInstrumentation extends KeyValueServiceInstrumentation {

        DummyKeyValueServiceInstrumentation(int kvsPort, String dockerComposeFileName) {
            super(kvsPort, dockerComposeFileName);
        }

        @Override
        public KeyValueServiceConfig getKeyValueServiceConfig(InetSocketAddress addr) {
            return null;
        }

        @Override
        public Optional<KeyValueServiceRuntimeConfig> getKeyValueServiceRuntimeConfig(InetSocketAddress addr) {
            return Optional.empty();
        }

        @Override
        public boolean canConnect(InetSocketAddress addr) {
            return false;
        }

        @Override
        public String toString() {
            return "dummy";
        }
    }
}
