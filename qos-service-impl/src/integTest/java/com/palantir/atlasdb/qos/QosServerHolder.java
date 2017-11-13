/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.qos;

import org.junit.rules.ExternalResource;

import com.palantir.atlasdb.qos.server.QosServerConfig;
import com.palantir.atlasdb.qos.server.QosServerLauncher;

import io.dropwizard.testing.DropwizardTestSupport;
import io.dropwizard.testing.ResourceHelpers;

public class QosServerHolder extends ExternalResource {
    private final DropwizardTestSupport<QosServerConfig> testSupport;

    QosServerHolder(String configFileName) {
        this.testSupport = new DropwizardTestSupport<>(
                QosServerLauncher.class, ResourceHelpers.resourceFilePath(configFileName));
    }

    @Override
    protected void before() throws Exception {
        testSupport.before();
    }

    @Override
    protected void after() {
        testSupport.after();
    }
}
