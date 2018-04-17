/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api;

import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

public class TableReferenceAndCellTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModule(new AfterburnerModule());

    @Test
    public void serializationDeserializationTest() throws IOException {
        TableReference tableReference = TableReference.createFromFullyQualifiedName("abc.def");
        Cell cell = Cell.create(new byte[] {0, 0}, new byte[] {1, 1});
        TableReferenceAndCell tableRefCell = ImmutableTableReferenceAndCell.builder()
                .tableRef(tableReference)
                .cell(cell)
                .build();

        byte[] valueAsBytes = OBJECT_MAPPER.writeValueAsBytes(tableRefCell);
        Assertions.assertThat(OBJECT_MAPPER.readValue(valueAsBytes, TableReferenceAndCell.class))
                .isEqualTo(tableRefCell);
    }
}
