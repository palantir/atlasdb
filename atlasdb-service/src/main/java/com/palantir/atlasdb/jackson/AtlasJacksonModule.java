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
package com.palantir.atlasdb.jackson;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.palantir.atlasdb.api.RangeToken;
import com.palantir.atlasdb.api.TableCell;
import com.palantir.atlasdb.api.TableCellVal;
import com.palantir.atlasdb.api.TableRange;
import com.palantir.atlasdb.api.TableRowResult;
import com.palantir.atlasdb.api.TableRowSelection;
import com.palantir.atlasdb.impl.TableMetadataCache;
import com.palantir.atlasdb.table.description.TableMetadata;
import javax.inject.Inject;

public class AtlasJacksonModule {
    private final TableMetadataCache cache;

    @Inject
    public AtlasJacksonModule(TableMetadataCache cache) {
        this.cache = cache;
    }

    public Module createModule() {
        SimpleModule module = new SimpleModule("Atlas");
        module.addSerializer(new RangeTokenSerializer());
        module.addSerializer(new TableCellSerializer(cache));
        module.addSerializer(new TableCellValSerializer(cache));
        module.addSerializer(new TableMetadataSerializer());
        module.addSerializer(new TableRangeSerializer(cache));
        module.addSerializer(new TableRowResultSerializer(cache));
        module.addSerializer(new TableRowSelectionSerializer(cache));
        module.addDeserializer(RangeToken.class, new RangeTokenDeserializer());
        module.addDeserializer(TableCell.class, new TableCellDeserializer(cache));
        module.addDeserializer(TableCellVal.class, new TableCellValDeserializer(cache));
        module.addDeserializer(TableMetadata.class, new TableMetadataDeserializer());
        module.addDeserializer(TableRange.class, new TableRangeDeserializer(cache));
        module.addDeserializer(TableRowResult.class, new TableRowResultDeserializer(cache));
        module.addDeserializer(TableRowSelection.class, new TableRowSelectionDeserializer(cache));
        return module;
    }
}
