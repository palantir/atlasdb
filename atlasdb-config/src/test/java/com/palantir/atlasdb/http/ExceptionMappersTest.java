/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HttpHeaders;
import com.palantir.remoting2.errors.SerializableError;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.junit.Test;
import wiremock.org.eclipse.jetty.http.HttpStatus;

public class ExceptionMappersTest {
    private static final Exception RUNTIME_EXCEPTION = new RuntimeException("foo");
    private static final Response RESPONSE_503_WITHOUT_RETRY_AFTER =
            ExceptionMappers.encode503ResponseWithoutRetryAfter(RUNTIME_EXCEPTION);
    private static final Response RESPONSE_503_WITH_RETRY_AFTER =
            ExceptionMappers.encode503ResponseWithRetryAfter(RUNTIME_EXCEPTION);

    @Test
    public void responseWithoutRetryAfterShouldHaveContentTypeApplicationJson() {
        assertThat(RESPONSE_503_WITHOUT_RETRY_AFTER.getStringHeaders())
                .containsEntry(HttpHeaders.CONTENT_TYPE, ImmutableList.of(MediaType.APPLICATION_JSON));
    }

    @Test
    public void responseWithoutRetryAfterShouldHaveClaimedStatusCode() {
        assertThat(RESPONSE_503_WITHOUT_RETRY_AFTER.getStatus()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE_503);
    }

    @Test
    public void responseWithoutRetryAfterShouldHaveCorrespondingSerializableException() {
        assertThat(RESPONSE_503_WITHOUT_RETRY_AFTER.getEntity())
                .isInstanceOf(SerializableError.class)
                .satisfies(ExceptionMappersTest::assertSerializedFormOfRuntimeException);
    }

    @Test
    public void responseWithoutRetryAfterShouldNotHaveRetryAfterHeader() {
        assertThat(RESPONSE_503_WITHOUT_RETRY_AFTER.getStringHeaders()).doesNotContainKey(HttpHeaders.RETRY_AFTER);
    }

    @Test
    public void responseWithRetryAfterShouldHaveContentTypeApplicationJson() {
        assertThat(RESPONSE_503_WITH_RETRY_AFTER.getStringHeaders())
                .containsEntry(HttpHeaders.CONTENT_TYPE, ImmutableList.of(MediaType.APPLICATION_JSON));
    }

    @Test
    public void responseWithRetryAfterShouldHaveClaimedStatusCode() {
        assertThat(RESPONSE_503_WITH_RETRY_AFTER.getStatus()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE_503);
    }

    @Test
    public void responseWithRetryAfterShouldHaveCorrespondingSerializableException() {
        assertThat(RESPONSE_503_WITH_RETRY_AFTER.getEntity())
                .isInstanceOf(SerializableError.class)
                .satisfies(ExceptionMappersTest::assertSerializedFormOfRuntimeException);
    }

    @Test
    public void responseWithRetryAfterShouldHaveRetryAfterHeader() {
        assertThat(RESPONSE_503_WITH_RETRY_AFTER.getStringHeaders())
                .containsEntry(HttpHeaders.RETRY_AFTER, ImmutableList.of("0"));
    }

    private static void assertSerializedFormOfRuntimeException(Object entity) {
        SerializableError error = (SerializableError) entity;
        assertThat(error.getErrorName()).isEqualTo(RUNTIME_EXCEPTION.getClass().getName());
        assertThat(error.getMessage()).isEqualTo(RUNTIME_EXCEPTION.getMessage());
    }
}
