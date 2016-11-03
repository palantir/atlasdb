/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.server;

import com.codahale.metrics.health.HealthCheck;
import com.palantir.atlasdb.factory.ServiceDiscoveringAtlasSupplier;

public class KeyValueServiceHealthCheck extends HealthCheck {
    private final ServiceDiscoveringAtlasSupplier atlasSupplier;

    public KeyValueServiceHealthCheck(ServiceDiscoveringAtlasSupplier atlasSupplier) {
        this.atlasSupplier = atlasSupplier;
    }

    @Override
    protected HealthCheck.Result check() throws Exception {
        atlasSupplier.getKeyValueService().getAllTableNames();
        return Result.healthy();
    }
}
