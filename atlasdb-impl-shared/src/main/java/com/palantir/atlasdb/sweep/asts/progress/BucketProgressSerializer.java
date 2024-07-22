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
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeUncheckedIoException;
import java.io.IOException;

final class BucketProgressSerializer {
    private final ObjectReader progressReader;
    private final ObjectWriter progressWriter;

    private BucketProgressSerializer(ObjectReader progressReader, ObjectWriter progressWriter) {
        this.progressReader = progressReader;
        this.progressWriter = progressWriter;
    }

    static BucketProgressSerializer create(ObjectMapper mapper) {
        return new BucketProgressSerializer(
                mapper.readerFor(BucketProgress.class), mapper.writerFor(BucketProgress.class));
    }

    BucketProgress deserializeProgress(byte[] value) {
        try {
            return progressReader.readValue(value);
        } catch (IOException e) {
            throw new SafeUncheckedIoException("Error deserializing bucket progress", e, SafeArg.of("bytes", value));
        }
    }

    byte[] serializeProgress(BucketProgress value) {
        try {
            return progressWriter.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw new SafeUncheckedIoException(
                    "Error serializing bucket progress", e, SafeArg.of("bucketProgress", value));
        }
    }
}
