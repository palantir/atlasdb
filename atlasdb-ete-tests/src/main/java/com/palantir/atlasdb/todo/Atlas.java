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

import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLSocketFactory;

import com.google.common.base.Optional;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;

import io.dropwizard.jersey.setup.JerseyEnvironment;

public class Atlas {

    private static final boolean NO_HIDDEN_TABLES = false;

    private final List<Todo> todos = new ArrayList<>();
    private final SerializableTransactionManager transactionManager;

    public Atlas(AtlasDbConfig config, JerseyEnvironment environment) {
        Optional<SSLSocketFactory> ssl = Optional.absent();
        Schema schema = AtlasTodosSchema.getSchema();
        transactionManager = TransactionManagers.create(config, ssl, schema, environment::register, NO_HIDDEN_TABLES);
    }

    public void addTodo(Todo todo) {
        todos.add(todo);
    }

    public List<Todo> listTodos() {
        return todos;
    }
}
