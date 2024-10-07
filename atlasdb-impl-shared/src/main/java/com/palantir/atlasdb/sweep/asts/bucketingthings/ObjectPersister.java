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

package com.palantir.atlasdb.sweep.asts.bucketingthings;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.errorprone.annotations.CompileTimeConstant;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeUncheckedIoException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.io.IOException;

// Without a type T, we can't safely implement tryDeserialize without an error of unsafe type cases.
// Having T also helps give some type safety to prevent accidental screwups when using the many reader writers.
final class ObjectPersister<T> {
    private static final SafeLogger log = SafeLoggerFactory.get(ObjectPersister.class);
    private final ObjectReader reader;
    private final ObjectWriter writer;
    private final Class<T> clazz; // Partly exists to "consume" T, but also so that our logs can be more specific.
    private final LogSafety logSafety;

    private ObjectPersister(ObjectReader reader, ObjectWriter writer, Class<T> clazz, LogSafety logSafety) {
        this.reader = reader;
        this.writer = writer;
        this.clazz = clazz;
        this.logSafety = logSafety;
    }

    static <T> ObjectPersister<T> of(Class<T> clazz, LogSafety logSafetyOfValues) {
        // Not taking an object mapper as a parameter to prevent bugs caused by passing in a non-order preserving one
        // when using this for CAS workflows.
        return new ObjectPersister<>(
                ConsistentOrderingObjectMapper.OBJECT_MAPPER.readerFor(clazz),
                ConsistentOrderingObjectMapper.OBJECT_MAPPER.writerFor(clazz),
                clazz,
                logSafetyOfValues);
    }

    public T tryDeserialize(byte[] bytes) {
        try {
            return reader.readValue(bytes);
        } catch (IOException e) {
            throw new SafeUncheckedIoException(
                    "Failed to deserialise {} into {}", e, toArg("bytes", bytes), SafeArg.of("class", clazz.getName()));
        }
    }

    public byte[] trySerialize(T object) {
        try {
            return writer.writeValueAsBytes(object);
        } catch (JsonProcessingException e) {
            throw new SafeUncheckedIoException(
                    "Failed to serialise {} from {}", e, toArg("object", object), SafeArg.of("class", clazz.getName()));
        }
    }

    // Log safety determined at runtime, and so cannot just blanket mark the function as unsafe as it would expect.
    @SuppressWarnings("SafeLoggingPropagation")
    private <K> Arg<K> toArg(@CompileTimeConstant String name, K value) {
        switch (logSafety) {
            case SAFE:
                return SafeArg.of(name, value);
            case UNSAFE:
                return UnsafeArg.of(name, value);
            default:
                log.error("Unexpected log safety level {}", SafeArg.of("logSafety", logSafety));
                // We're explicitly going against the grain and not throwing. This does mean that we'll fail
                // silently (other than a log message!), but it's better than causing an accidental outage for something
                // inconsequential.
                return UnsafeArg.of(name, value);
        }
    }

    enum LogSafety {
        SAFE,
        UNSAFE, // No DO_NOT_LOG, as we shouldn't be persisting things you cannot keep on disk safely!
    }
}
