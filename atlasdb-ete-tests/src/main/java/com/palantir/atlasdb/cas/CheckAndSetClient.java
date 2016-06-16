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
package com.palantir.atlasdb.cas;

import javax.net.ssl.SSLSocketFactory;

import com.google.common.base.Optional;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.transaction.api.TransactionManager;

import io.dropwizard.jersey.setup.JerseyEnvironment;

public class CheckAndSetClient {
    private static final boolean DONT_SHOW_HIDDEN_TABLES = false;
    private static final Optional<SSLSocketFactory> NO_SSL = Optional.absent();

    private final TransactionManager transactionManager;

    public CheckAndSetClient(AtlasDbConfig config, JerseyEnvironment environment) {
        Schema schema = CheckAndSetSchema.getSchema();
        transactionManager = TransactionManagers.create(config, NO_SSL, schema, environment::register, DONT_SHOW_HIDDEN_TABLES);
    }

    public Optional<Long> get() {
        return transactionManager.runTaskReadOnly((transaction) -> new CheckAndSetPersistentValue(transaction).get());
    }

    public void set(Optional<Long> value) {
        transactionManager.runTaskWithRetry((transaction) -> {
            new CheckAndSetPersistentValue(transaction).set(value);
            return null;
        });
    }

    public boolean checkAndSet(Optional<Long> oldValue, Optional<Long> newValue) {
        return transactionManager.runTaskWithRetry((transaction) -> new CheckAndSetPersistentValue(transaction).checkAndSet(oldValue, newValue));
    }
}
