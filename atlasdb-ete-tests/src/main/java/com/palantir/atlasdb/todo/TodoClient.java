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
package com.palantir.atlasdb.todo;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.net.ssl.SSLSocketFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.common.base.BatchingVisitable;

import io.dropwizard.jersey.setup.JerseyEnvironment;

public class TodoClient {
    private static final boolean DONT_SHOW_HIDDEN_TABLES = false;
    private static final Optional<SSLSocketFactory> NO_SSL = Optional.absent();

    private final SerializableTransactionManager transactionManager;
    private final Random random = new Random();

    public TodoClient(AtlasDbConfig config, JerseyEnvironment environment) {
        Schema schema = TodoSchema.getSchema();
        transactionManager = TransactionManagers.create(config, NO_SSL, schema, environment::register, DONT_SHOW_HIDDEN_TABLES);
    }

    public void addTodo(Todo todo) {
        transactionManager.runTaskWithRetry((transaction) -> {
            Cell thisCell = Cell.create(ValueType.FIXED_LONG.convertFromJava(random.nextLong()), TodoSchema.todoTextColumn());
            Map<Cell, byte[]> write = ImmutableMap.of(thisCell, ValueType.STRING.convertFromJava(todo.text()));

            transaction.put(TodoSchema.todoTable(), write);
            return null;
        });
    }

    public List<Todo> getTodoList() {
        ImmutableList<RowResult<byte[]>> results = transactionManager.runTaskWithRetry((transaction) -> {
            BatchingVisitable<RowResult<byte[]>> rowResultBatchingVisitable = transaction.getRange(TodoSchema.todoTable(), RangeRequest.all());
            ImmutableList.Builder<RowResult<byte[]>> rowResults = ImmutableList.builder();

            rowResultBatchingVisitable.batchAccept(1000, items -> {
                rowResults.addAll(items);
                return true;
            });

            return rowResults.build();
        });

        return results.stream()
                .map(RowResult::getOnlyColumnValue)
                .map(ValueType.STRING::convertToString)
                .map(ImmutableTodo::of)
                .collect(toList());
    }
}
