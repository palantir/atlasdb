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

import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import java.net.InetSocketAddress;
import org.mockito.Mockito;

public class MockKeyValueServiceInstrumentation extends KeyValueServiceInstrumentation {

    public MockKeyValueServiceInstrumentation() {
        super(0, "mock_filename");
    }

    @Override
    public KeyValueServiceConfig getKeyValueServiceConfig(InetSocketAddress addr) {
        return Mockito.mock(KeyValueServiceConfig.class);
    }

    @Override
    public boolean canConnect(InetSocketAddress addr) {
        return true;
    }

    @Override
    public String toString() {
        return "MOCK";
    }
}
