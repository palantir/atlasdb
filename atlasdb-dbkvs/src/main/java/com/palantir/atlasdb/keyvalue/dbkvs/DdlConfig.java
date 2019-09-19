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
package com.palantir.atlasdb.keyvalue.dbkvs;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.Preconditions;
import org.immutables.value.Value;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type", visible = false)
@JsonSubTypes({
        @JsonSubTypes.Type(PostgresDdlConfig.class),
        @JsonSubTypes.Type(OracleDdlConfig.class),
        @JsonSubTypes.Type(H2DdlConfig.class)})
public abstract class DdlConfig {
    public abstract String type();

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

    @Value.Check
    protected final void check() {
        Preconditions.checkState(
                metadataTable().getNamespace().isEmptyNamespace(),
                "'metadataTable' should have empty namespace'");
    }

    public interface Visitor<T> {
        T visit(PostgresDdlConfig postgresDdlConfig);
        T visit(H2DdlConfig h2DdlConfig);
        T visit(OracleDdlConfig oracleDdlConfig);
    }

    public abstract <T> T accept(Visitor<T> visitor);
}
