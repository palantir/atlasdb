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
package com.palantir.atlasdb.schema.stream;

import java.io.File;

import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ExpirationStrategy;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.schema.Namespace;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.ValueType;

public class StreamTestSchema implements AtlasSchema {
    public static final AtlasSchema INSTANCE = new StreamTestSchema();

    private static final Schema STREAM_TEST_SCHEMA = generateSchema();

    private static Schema generateSchema() {
        Schema schema = new Schema("StreamTest",
                StreamTest.class.getPackage().getName() + ".generated",
                Namespace.DEFAULT_NAMESPACE);

        schema.addStreamStoreDefinition("stream_test", "stream_test", ValueType.VAR_LONG, 4000);

        schema.addStreamStoreDefinition("stream_test_2", "stream_test_2", ValueType.VAR_LONG, 4000, ExpirationStrategy.INDIVIDUALLY_SPECIFIED, false, false);

        return schema;
    }

    public static Schema getSchema() {
        return STREAM_TEST_SCHEMA;
    }

    public static void main(String[]  args) throws Exception {
        STREAM_TEST_SCHEMA.renderTables(new File("src/test/java"));
    }

    @Override
    public Schema getLatestSchema() {
        return STREAM_TEST_SCHEMA;
    }

    @Override
    public Namespace getNamespace() {
        return Namespace.DEFAULT_NAMESPACE;
    }
}
