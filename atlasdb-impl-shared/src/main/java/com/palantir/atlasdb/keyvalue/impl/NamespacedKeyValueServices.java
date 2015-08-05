/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.impl;

import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.NamespacedKeyValueService;
import com.palantir.atlasdb.keyvalue.TableMappingService;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.timestamp.TimestampService;

/**
 * @author jweel
 */
public class NamespacedKeyValueServices {
    public static KeyValueService wrapWithNamespaceMappingKvs(KeyValueService keyValueService,
                                                              final TimestampService timestampService) {
        TableMappingService tableMap = KVTableMappingService.create(
                keyValueService,
                new Supplier<Long>() {
                    @Override
                    public Long get() {
                        return timestampService.getFreshTimestamp();
                    }
                });
        NamespacedKeyValueService namespacedKeyValueService = TableRemappingKeyValueService.create(
                keyValueService,
                tableMap);
        KeyValueService namespaceMappingKeyvalueService = NamespaceMappingKeyValueService.create(namespacedKeyValueService);
        return namespaceMappingKeyvalueService;
    }
}
