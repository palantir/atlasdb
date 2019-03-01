/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.ete;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.rows.GetRowKeysResource;
import com.palantir.common.random.RandomBytes;

public class CassandraGetRowKeysEteTest {
    private GetRowKeysResource client = EteSetup.createClientToSingleNode(GetRowKeysResource.class);

    @Before
    public void setup() {
        client.resetTable();
    }

    @Test
    public void canGetRowKeysOnEmptyTable() {
        assertThat(client.getRowKeys(0, 10)).isEmpty();
    }

    @Test
    public void getRowKeysReturnsOneResultPerRowStartingWithStartRow() {
        generateCellsWithNumberedRows(3).forEach(client::insertCell);
        generateCellsWithNumberedRows(5).forEach(client::insertCell);
        generateCellsWithNumberedRows(4).forEach(client::insertCell);
        assertListsContainsExactlyRowsFromTo(client.getRowKeys(1, 10), 1, 5);
    }

    @Test
    public void getRowKeysReturnsRequestedNumberOfRowsStartingWithStartRow() {
        generateCellsWithNumberedRows(20).forEach(client::insertCell);
        assertListsContainsExactlyRowsFromTo(client.getRowKeys(7, 10), 7, 17);
    }

    private static List<Cell> generateCellsWithNumberedRows(int numRows) {
        return IntStream.range(0, numRows)
                .mapToObj(CassandraGetRowKeysEteTest::generateCell)
                .collect(Collectors.toList());
    }

    private static Cell generateCell(int rowName) {
        return Cell.create(PtBytes.toBytes(rowName), RandomBytes.ofLength(30));
    }

    private void assertListsContainsExactlyRowsFromTo(List<byte[]> result, int fromInclusive, int toExclusive) {
        List<byte[]> expected = IntStream.range(fromInclusive, toExclusive)
                .mapToObj(PtBytes::toBytes)
                .collect(Collectors.toList());
        assertThat(result.size()).isEqualTo(expected.size());
        for (int i = 0; i < result.size(); i++) {
            assertThat(result.get(i)).containsExactly(expected.get(i));
        }
    }
}
