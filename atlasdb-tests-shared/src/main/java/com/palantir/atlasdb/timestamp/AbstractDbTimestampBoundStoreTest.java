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
package com.palantir.atlasdb.timestamp;

import com.palantir.timestamp.TimestampBoundStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class AbstractDbTimestampBoundStoreTest {
    protected TimestampBoundStore store;

    @Before
    public void setUp() throws Exception {
        store = createTimestampBoundStore();
    }

    protected abstract TimestampBoundStore createTimestampBoundStore();

    @Test
    public void testTimestampBoundStore() {
        long upperLimit1 = store.getUpperLimit();
        long upperLimit2 = store.getUpperLimit();
        Assert.assertEquals(upperLimit1, upperLimit2);
        store.storeUpperLimit(upperLimit2 + 1);
        long upperLimit3 = store.getUpperLimit();
        Assert.assertEquals(upperLimit3, upperLimit2 + 1);
    }
}
