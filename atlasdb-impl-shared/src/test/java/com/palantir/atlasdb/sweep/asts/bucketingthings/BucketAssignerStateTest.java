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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.BaseEncoding;
import com.palantir.conjure.java.serialization.ObjectMappers;
import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class BucketAssignerStateTest {
    private static final ObjectMapper OBJECT_MAPPER = ObjectMappers.newSmileServerObjectMapper();

    // Think _very_ carefully about changing this without a migration.
    private static final byte[] START_SERIALIZED =
            BaseEncoding.base64().decode("OikKBfqDdHlwZURzdGFydJZzdGFydFRpbWVzdGFtcEluY2x1c2l2ZcL7");
    private static final byte[] OPENING_SERIALIZED =
            BaseEncoding.base64().decode("OikKBfqDdHlwZUZvcGVuaW5nlnN0YXJ0VGltZXN0YW1wSW5jbHVzaXZlxPs=");
    private static final byte[] WAITING_UNTIL_CLOSED_SERIALIZED = BaseEncoding.base64()
            .decode("OikKBfqDdHlwZVR3YWl0aW5nVW50aWxDbG9zZWFibGWWc3RhcnRUaW1lc3RhbXBJbmNsdXNpdmXG+w==");
    private static final byte[] CLOSING_FROM_OPEN_SERIALIZED = BaseEncoding.base64()
            .decode(
                    "OikKBfqDdHlwZU5jbG9zaW5nRnJvbU9wZW6Wc3RhcnRUaW1lc3RhbXBJbmNsdXNpdmXIlGVuZFRpbWVzdGFtcEV4Y2x1c2l2Zcr7");
    private static final byte[] IMMEDIATELY_CLOSING_SERIALIZED = BaseEncoding.base64()
            .decode(
                    "OikKBfqDdHlwZVFpbW1lZGlhdGVseUNsb3NpbmeWc3RhcnRUaW1lc3RhbXBJbmNsdXNpdmXMlGVuZFRpbWVzdGFtcEV4Y2x1c2l2Zc77");

    @ParameterizedTest(name = "{0}")
    @MethodSource("bucketAssignerStates")
    public void canRoundTripSerdeState(BucketAssignerState state, byte[] _unused) throws IOException {
        byte[] json = OBJECT_MAPPER.writeValueAsBytes(state);
        BucketAssignerState deserialized = OBJECT_MAPPER.readValue(json, BucketAssignerState.class);
        assertThat(deserialized).isEqualTo(state);
        // Adding instance of check to be _really_ sure, since a mistake here would be very painful.
        assertThat(deserialized).isInstanceOf(state.getClass());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("bucketAssignerStates")
    public void canDeserializeExistingVersion(BucketAssignerState bucketProgress, byte[] serializedForm)
            throws IOException {
        assertThat(OBJECT_MAPPER.readValue(serializedForm, BucketAssignerState.class))
                .isEqualTo(bucketProgress);
    }

    private static Stream<Arguments> bucketAssignerStates() {
        return Stream.of(
                Arguments.of(BucketAssignerState.start(1), START_SERIALIZED),
                Arguments.of(BucketAssignerState.opening(2), OPENING_SERIALIZED),
                Arguments.of(BucketAssignerState.waitingUntilCloseable(3), WAITING_UNTIL_CLOSED_SERIALIZED),
                Arguments.of(BucketAssignerState.closingFromOpen(4, 5), CLOSING_FROM_OPEN_SERIALIZED),
                Arguments.of(BucketAssignerState.immediatelyClosing(6, 7), IMMEDIATELY_CLOSING_SERIALIZED));
    }
}
