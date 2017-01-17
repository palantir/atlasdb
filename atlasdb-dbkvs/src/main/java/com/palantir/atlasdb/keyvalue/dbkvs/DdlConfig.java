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
package com.palantir.atlasdb.keyvalue.dbkvs;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbTableFactory;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type", visible = false)
@JsonSubTypes({
        @JsonSubTypes.Type(PostgresDdlConfig.class),
        @JsonSubTypes.Type(OracleDdlConfig.class),
        @JsonSubTypes.Type(H2DdlConfig.class)})
public abstract class DdlConfig {
    public abstract String type();

    public abstract Supplier<DbTableFactory> tableFactorySupplier();

    public abstract TableReference metadataTable();

    @Value.Default
    public String tablePrefix() {
        return "";
    }

    @Value.Default
    public int poolSize() {
        return 64;
    }

    @Value.Default
    public int fetchBatchSize() {
        return 256;
    }

    @Value.Default
    public int mutationBatchCount() {
        return 1000;
    }

    @Value.Default
    public int mutationBatchSizeBytes() {
        return 2 * 1024 * 1024;
    }

    @Value.Default
    @JsonIgnore
    public boolean useTableMapping() {
        return true;
    }

    @Value.Check
    protected final void check() {
        Preconditions.checkState(
                metadataTable().getNamespace().isEmptyNamespace(),
                "'metadataTable' should have empty namespace'");
    }
}
