/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.cleaner;

import java.util.concurrent.ExecutorService;

import org.junit.After;
import org.junit.Before;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.common.concurrent.PTExecutors;

public class InMemorySweeperTest extends AbstractSweeperTest {
    private ExecutorService exec;

    @Before
    @SuppressWarnings("serial")
    public void setup() {
        exec = PTExecutors.newCachedThreadPool();
        KeyValueService kvs = new InMemoryKeyValueService(false, exec);
        super.globalTestSetup(kvs);
    }

    @Override
    @After
    public void tearDown() {
        super.tearDown();
        exec.shutdown();
    }
}
