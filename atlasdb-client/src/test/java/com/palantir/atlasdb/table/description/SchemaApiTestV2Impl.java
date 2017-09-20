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

package com.palantir.atlasdb.table.description;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.table.description.generated.SchemaApiTestTable;
import com.palantir.atlasdb.table.description.generated.SchemaApiTestV2Table;
import com.palantir.atlasdb.table.description.generated.ApiTestTableFactory;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.base.BatchingVisitableView;
import com.palantir.util.Pair;

public class SchemaApiTestV2Impl extends AbstractSchemaApiTest {

    private static final ApiTestTableFactory tableFactory = ApiTestTableFactory.of();

    @Override
    protected void putSingleRowFirstColumn(Transaction transaction, String rowKey, long value) {
        SchemaApiTestV2Table table = tableFactory.getSchemaApiTestV2Table(transaction);
        table.putColumn1(rowKey, value);
    }

    @Override
    protected Long getSingleRowFirstColumn(Transaction transaction, String rowKey) {
        SchemaApiTestV2Table table = tableFactory.getSchemaApiTestV2Table(transaction);
        Optional<Long> result = table.getColumn1(rowKey);
        return result.get();
    }

    @Override
    protected List<Long> getMultipleRowsFirstColumn(Transaction transaction, List<String> rowKeys) {
        SchemaApiTestV2Table table = tableFactory.getSchemaApiTestV2Table(transaction);
        return new ArrayList<>(table.getColumn1(rowKeys).values());
    }

    @Override
    protected List<String> getRangeSecondColumn(Transaction transaction, String startRowKey, String endRowKey) {
        SchemaApiTestV2Table table = tableFactory.getSchemaApiTestV2Table(transaction);

        RangeRequest rangeRequest = RangeRequest.builder()
                .startRowInclusive(SchemaApiTestTable.SchemaApiTestRow.of(startRowKey).persistToBytes())
                .endRowExclusive(SchemaApiTestTable.SchemaApiTestRow.of(endRowKey).persistToBytes())
                .build();

        BatchingVisitableView<Pair<String, String>> rangeRequestResult = table.getRangeColumn2(rangeRequest);
        return rangeRequestResult.immutableCopy().stream().map(Pair::getRhSide).collect(Collectors.toList());
    }

    @Override
    protected void deleteWholeRow(Transaction transaction, String rowKey) {
        SchemaApiTestV2Table table = tableFactory.getSchemaApiTestV2Table(transaction);
        table.deleteRow(rowKey);
    }

    @Override
    protected void deleteFirstColumn(Transaction transaction, String rowKey) {
        SchemaApiTestV2Table table = tableFactory.getSchemaApiTestV2Table(transaction);
        table.deleteColumn1(rowKey);
    }
}
