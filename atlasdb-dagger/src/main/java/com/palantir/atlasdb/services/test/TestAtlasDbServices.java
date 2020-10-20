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
package com.palantir.atlasdb.services.test;

import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.services.KeyValueServiceModule;
import com.palantir.atlasdb.services.LockAndTimestampModule;
import com.palantir.atlasdb.services.MetricsModule;
import com.palantir.atlasdb.services.RawKeyValueServiceModule;
import com.palantir.atlasdb.services.ServicesConfigModule;
import com.palantir.lock.LockClient;
import dagger.Component;
import javax.inject.Singleton;

@Singleton
@Component(
        modules = {
            ServicesConfigModule.class,
            KeyValueServiceModule.class,
            RawKeyValueServiceModule.class,
            LockAndTimestampModule.class,
            MetricsModule.class,
            TestSweeperModule.class,
            TestTransactionManagerModule.class
        })
public abstract class TestAtlasDbServices extends AtlasDbServices {

    public abstract LockClient getTestLockClient();
}
