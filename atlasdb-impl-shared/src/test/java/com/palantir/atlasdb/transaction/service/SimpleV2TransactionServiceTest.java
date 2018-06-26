/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;

public class SimpleV2TransactionServiceTest {
    @Test
    public void t() {
        SimpleV2TransactionService s = new SimpleV2TransactionService(new InMemoryKeyValueService(false));
        for (int i = 0; i < 1000; i++) {
            long x = Math.abs(ThreadLocalRandom.current().nextLong());
            Cell c = s.getTransactionCell(x);
            assertThat(s.getTimestampFromCell(c)).isEqualTo(x);
        }
    }
}
