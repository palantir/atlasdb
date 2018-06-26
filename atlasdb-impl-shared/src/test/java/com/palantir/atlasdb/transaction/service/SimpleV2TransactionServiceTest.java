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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;

public class SimpleV2TransactionServiceTest {
    @Test
    public void decodeOnEncodeIsAnIdentityFunction() {
        SimpleV2TransactionService service = new SimpleV2TransactionService(new InMemoryKeyValueService(false));
        for (int i = 0; i < 1000; i++) {
            long testNumber = Math.abs(ThreadLocalRandom.current().nextLong());
            Cell cell = service.encodeTimestampAsCell(testNumber);
            assertThat(service.decodeCellAsTimestamp(cell)).isEqualTo(testNumber);
        }
    }

    @Test
    public void uniformlyPartitionsValues() {
        SimpleV2TransactionService service = new SimpleV2TransactionService(new InMemoryKeyValueService(false));
        List<Cell> cellList = Lists.newArrayList();
        for (int i = 0; i < 256 * 256; i++) {
            Cell cell = service.encodeTimestampAsCell(i);
            cellList.add(cell);
        }
        Map<Byte, Integer> leadingByteCounts = cellList.stream()
                .collect(Collectors.groupingBy(cell -> cell.getRowName()[0]))
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().size()));

        assertThat(leadingByteCounts.size()).isEqualTo(256);
        assertThat(leadingByteCounts.values()).containsOnly(256);
    }
}
