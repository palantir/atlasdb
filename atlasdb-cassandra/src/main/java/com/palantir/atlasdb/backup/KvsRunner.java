/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.backup;

import com.palantir.atlasdb.backup.api.AtlasService;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import java.util.function.Function;

public interface KvsRunner {
    <T> T run(AtlasService atlasService, Function<KeyValueService, T> function);

    static KvsRunner create(Function<AtlasService, KeyValueService> kvsFactory) {
        return new ClosingKvsRunner(kvsFactory);
    }

    static KvsRunner create(TransactionManager txnManager) {
        return new NonClosingKvsRunner(txnManager);
    }
}
