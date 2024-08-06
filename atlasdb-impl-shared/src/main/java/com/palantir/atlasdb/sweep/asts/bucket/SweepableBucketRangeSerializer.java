/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.asts.bucket;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.IOException;

final class SweepableBucketRangeSerializer {
    private final ObjectReader reader;
    private final ObjectWriter writer;

    private SweepableBucketRangeSerializer(ObjectReader reader, ObjectWriter writer) {
        this.reader = reader;
        this.writer = writer;
    }

    static SweepableBucketRangeSerializer create(ObjectMapper mapper) {
        return new SweepableBucketRangeSerializer(mapper.readerFor(SweepableBucketRangeSerializer.class),
                mapper.writer());
    }

    byte[] serialize(SweepableBucketRange range) {
        try {
            return writer.writeValueAsBytes(range);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    SweepableBucketRange deserialize(byte[] data) {
        try {
            return reader.readValue(data, SweepableBucketRange.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
