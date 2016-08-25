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
package com.palantir.atlasdb.jackson;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.api.TableRange;
import com.palantir.atlasdb.encoding.PtBytes;

public class TableRangeSerializer extends StdSerializer<TableRange> {
    private static final long serialVersionUID = 1L;

    public TableRangeSerializer() {
        super(TableRange.class);
    }

    @Override
    public void serialize(TableRange value,
                          JsonGenerator jgen,
                          SerializerProvider provider) throws IOException, JsonGenerationException {
        jgen.writeStartObject(); {
            jgen.writeStringField("table", value.getTableName());
            jgen.writeBinaryField("raw_start", value.getStartRow());
            jgen.writeBinaryField("raw_end", value.getEndRow());
            jgen.writeNumberField("batch_size", value.getBatchSize());
            if (!Iterables.isEmpty(value.getColumns())) {
                jgen.writeArrayFieldStart("cols"); {
                    for (byte[] column : value.getColumns()) {
                        jgen.writeString(PtBytes.toString(column));
                    }
                } jgen.writeEndArray();
            }
        } jgen.writeEndObject();
    }
}
