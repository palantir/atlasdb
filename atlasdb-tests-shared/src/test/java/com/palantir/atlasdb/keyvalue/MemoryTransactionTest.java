/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTest;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.remoting2.tracing.Tracers;

public class MemoryTransactionTest extends AbstractTransactionTest {
    @Override
    protected KeyValueService getKeyValueService() {
        return new InMemoryKeyValueService(false,
                Tracers.wrap(PTExecutors.newSingleThreadExecutor(PTExecutors.newNamedThreadFactory(false))));
    }
}
