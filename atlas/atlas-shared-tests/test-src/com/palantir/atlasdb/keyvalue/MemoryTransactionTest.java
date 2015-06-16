// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.keyvalue;

import java.util.concurrent.ExecutorService;

import org.junit.BeforeClass;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTest;
import com.palantir.common.concurrent.PTExecutors;

public class MemoryTransactionTest extends AbstractTransactionTest {

    static ExecutorService executor;
    @BeforeClass
    public static void setupExecutor() {
        executor = PTExecutors.newFixedThreadPool(16, PTExecutors.newNamedThreadFactory(true));
    }

    @Override
    protected KeyValueService getKeyValueService() {
        return new InMemoryKeyValueService(false, executor);
    }

}
