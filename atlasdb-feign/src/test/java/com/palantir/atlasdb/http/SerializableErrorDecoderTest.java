/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.http;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.ws.rs.core.MediaType;

import org.apache.http.HttpStatus;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.net.HttpHeaders;
import com.palantir.atlasdb.http.errors.AtlasDbRemoteException;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.remoting2.errors.SerializableError;
import com.palantir.remoting2.ext.jackson.ObjectMappers;

import feign.Response;

@SuppressWarnings("ThrowableNotThrown")
public class SerializableErrorDecoderTest {
    private static final ObjectMapper OBJECT_MAPPER = ObjectMappers.newClientObjectMapper();

    private static final String EMPTY_REASON = "";
    private static final byte[] EMPTY_BODY = new byte[0];
    private static final String SOME_METHOD_KEY = "METHOD";
    private static final String EXCEPTION_MESSAGE = "foo";
    private static final String LOCK_ID = "lock";

    private static final String KNOWN_ERROR_NAME = IllegalArgumentException.class.getCanonicalName();
    private static final String UNKNOWN_ERROR_NAME = "com.palantir.atlasdb.errors.one.two.three.four";
    private static final String NOT_ERROR_NAME = "java.lang.Integer";

    private final SerializableErrorDecoder decoder = new SerializableErrorDecoder();

    @Test
    public void canDecodeExceptionOfKnownType() throws JsonProcessingException {
        assertCanSerializeAndDeserializeErrorWithName(KNOWN_ERROR_NAME);
    }

    @Test
    public void canDecodeExceptionOfUnknownType() throws JsonProcessingException {
        assertCanSerializeAndDeserializeErrorWithName(UNKNOWN_ERROR_NAME);
    }

    @Test
    public void canDecodeExceptionBelievedToNotBeAnExceptionType() throws JsonProcessingException {
        assertCanSerializeAndDeserializeErrorWithName(NOT_ERROR_NAME);
    }

    @Test
    public void resilientToEmptyBody() {
        Response response = createResponse(HttpStatus.SC_SERVICE_UNAVAILABLE, EMPTY_BODY);

        assertCanDecodeRuntimeException(response);
    }

    @Test
    public void resilientToMalformedBody() {
        byte[] oneByteArray = {42};
        Response response = createResponse(HttpStatus.SC_SERVICE_UNAVAILABLE, oneByteArray);

        assertCanDecodeRuntimeException(response);
    }

    @Test
    public void resilientToValidJsonBodyThatIsNotASerializableError() throws JsonProcessingException {
        LockRequest lockRequest =
                LockRequest.builder(ImmutableSortedMap.of(StringLockDescriptor.of(LOCK_ID), LockMode.WRITE))
                        .build();
        Response response = createResponseForEntity(lockRequest);

        assertCanDecodeRuntimeException(response);
    }

    @Test
    public void resilientToHttpRemoting3SerializableErrors() throws IOException {
        Path filePath = Paths.get("src", "test", "resources", "remoting3-exception.json");
        Response response = createResponse(HttpStatus.SC_SERVICE_UNAVAILABLE, Files.readAllBytes(filePath));

        assertCanDecodeRuntimeException(response);
    }

    private void assertCanSerializeAndDeserializeErrorWithName(String errorName) throws JsonProcessingException {
        SerializableError serializableError = SerializableError.of(EXCEPTION_MESSAGE, errorName);
        Response response = createResponseForEntity(serializableError);

        Exception exception = decoder.decode(SOME_METHOD_KEY, response);
        assertThat(exception).isInstanceOf(AtlasDbRemoteException.class)
                .satisfies(remoteException -> assertHasErrorName(remoteException, errorName));
    }

    private static Response createResponseForEntity(Object entity) throws JsonProcessingException {
        return createResponse(HttpStatus.SC_SERVICE_UNAVAILABLE, OBJECT_MAPPER.writeValueAsBytes(entity));
    }

    private static Response createResponse(int status, byte[] bodyBytes) {
        return Response.create(
                status,
                EMPTY_REASON,
                ImmutableMap.of(HttpHeaders.CONTENT_TYPE, ImmutableSet.of(MediaType.APPLICATION_JSON)),
                bodyBytes);
    }

    private static void assertHasErrorName(Throwable exception, String expectedErrorName) {
        AtlasDbRemoteException remoteException = (AtlasDbRemoteException) exception;
        assertThat(remoteException.getErrorName()).isEqualTo(expectedErrorName);
    }

    private void assertCanDecodeRuntimeException(Response response) {
        Exception exception = decoder.decode(SOME_METHOD_KEY, response);
        assertThat(exception).isInstanceOf(RuntimeException.class);
    }
}
