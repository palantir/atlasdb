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

package com.palantir.atlasdb.sweep.asts.progress;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.io.IOException;

final class BucketProgressSerialization {
    private static final SafeLogger log = SafeLoggerFactory.get(BucketProgressSerialization.class);

    private static final ObjectMapper OBJECT_MAPPER = ObjectMappers.newServerObjectMapper();
    private static final ObjectReader OBJECT_READER = OBJECT_MAPPER.readerFor(BucketProgress.class);
    private static final ObjectWriter OBJECT_WRITER = OBJECT_MAPPER.writerFor(BucketProgress.class);

    private BucketProgressSerialization() {
        // utility class
    }

    static BucketProgress deserialize(byte[] value) {
        try {
            return OBJECT_READER.readValue(value);
        } catch (IOException e) {
            log.error("Error encountered when deserializing bucket progress", e);
            throw new RuntimeException(e);
        }
    }

    static byte[] serialize(BucketProgress value) {
        try {
            return OBJECT_WRITER.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            log.error("Error encountered when serializing bucket progress", e);
            throw new RuntimeException(e);
        }
    }
}
