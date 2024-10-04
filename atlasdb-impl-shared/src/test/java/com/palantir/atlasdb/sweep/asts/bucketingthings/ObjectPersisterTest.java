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

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.palantir.atlasdb.sweep.asts.bucketingthings.ObjectPersister.LogSafety;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import java.io.IOException;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.immutables.value.Value;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public final class ObjectPersisterTest {
    @ParameterizedTest
    @MethodSource("safety")
    public void failedSerializationWithLogSafetyThrowsExceptionWithCorrectArgSafety(SafetyAndFactory safetyAndFactory) {
        ObjectPersister<ImpossibleToSerialize> persister = ObjectPersister.of(
                ObjectMappers.newServerSmileMapper(), ImpossibleToSerialize.class, safetyAndFactory.safety());
        ImpossibleToSerialize object = ImpossibleToSerialize.of();
        assertThatLoggableExceptionThrownBy(() -> persister.trySerialize(object))
                .hasLogMessage("Failed to serialise {} from {}")
                .hasExactlyArgs(
                        safetyAndFactory.createArg("object", object),
                        SafeArg.of("class", ImpossibleToSerialize.class.getName()));
    }

    @ParameterizedTest
    @MethodSource("safety")
    public void failedDeserializationWithLogSafetyThrowsExceptionWithCorrectArgSafety(
            SafetyAndFactory safetyAndFactory) {
        ObjectPersister<ImpossibleToSerialize> persister = ObjectPersister.of(
                ObjectMappers.newServerSmileMapper(), ImpossibleToSerialize.class, safetyAndFactory.safety());
        byte[] bytes = new byte[] {1, 2, 3};
        assertThatLoggableExceptionThrownBy(() -> persister.tryDeserialize(bytes))
                .hasLogMessage("Failed to deserialise {} into {}")
                .hasExactlyArgs(
                        safetyAndFactory.createArg("bytes", bytes),
                        SafeArg.of("class", ImpossibleToSerialize.class.getName()));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, Long.MAX_VALUE, Long.MIN_VALUE}) // arbitrary
    public void deserialisationIsInverseOfSerialisation(long value) {
        ObjectPersister<Long> persister =
                ObjectPersister.of(ObjectMappers.newServerSmileMapper(), Long.class, LogSafety.SAFE);
        assertThat(persister.tryDeserialize(persister.trySerialize(value))).isEqualTo(value);
    }

    private static Stream<SafetyAndFactory> safety() {
        return Stream.of(
                ImmutableSafetyAndFactory.of(LogSafety.SAFE, SafeArg::of),
                ImmutableSafetyAndFactory.of(LogSafety.UNSAFE, UnsafeArg::of));
    }

    @Value.Immutable
    @JsonSerialize(using = FailingSerializer.class)
    //  Without any annotation, the object mapper will serialize as {}, rather than failing
    interface ImpossibleToSerialize {
        @Value.Parameter
        long value();

        static ImpossibleToSerialize of() {
            return ImmutableImpossibleToSerialize.of(123);
        }
    }

    static class FailingSerializer extends StdSerializer<ImpossibleToSerialize> {

        protected FailingSerializer(Class<ImpossibleToSerialize> t) {
            super(t);
        }

        @Override
        public void serialize(
                ImpossibleToSerialize impossibleToSerialize,
                JsonGenerator jsonGenerator,
                SerializerProvider serializerProvider)
                throws IOException {
            throw new IOException("This is a failing serializer");
        }
    }

    @Value.Immutable
    interface SafetyAndFactory {
        @Value.Parameter
        LogSafety safety();

        @Value.Parameter
        BiFunction<String, Object, Arg<?>> argFactory();

        default Arg<?> createArg(String name, Object value) {
            return argFactory().apply(name, value);
        }
    }
}
