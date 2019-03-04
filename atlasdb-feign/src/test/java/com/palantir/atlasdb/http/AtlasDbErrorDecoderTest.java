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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HttpHeaders;

import feign.Response;
import feign.RetryableException;
import feign.codec.ErrorDecoder;

public class AtlasDbErrorDecoderTest {
    private static final String EMPTY_METHOD_KEY = "";

    private static final Exception NON_RETRYABLE_EXCEPTION = new Exception();
    private static final Exception RETRYABLE_EXCEPTION = new RetryableException("MyException", new Date());

    private static final Date RETRY_AFTER_DATE = new Date(314159265L);

    private static final Map<String, Collection<String>> EMPTY_HEADERS = ImmutableMap.of();
    private static final Map<String, Collection<String>> HEADERS_WITH_RETRY_AFTER = ImmutableMap.of(
            HttpHeaders.RETRY_AFTER, ImmutableList.of(String.valueOf(RETRY_AFTER_DATE.getTime())));
    private static final String EMPTY_REASON = "";
    private static final byte[] EMPTY_BODY = new byte[0];

    private static final int STATUS_503 = 503;
    private static final int STATUS_429 = 429;
    private static final int STATUS_NOT_503_NOR_429 = 511;

    ErrorDecoder defaultDecoder;
    AtlasDbErrorDecoder atlasDbDecoder;

    @Before
    public void setUp() {
        defaultDecoder = mock(ErrorDecoder.class);
        atlasDbDecoder = new AtlasDbErrorDecoder(defaultDecoder);
    }

    // 429 tests
    @Test
    public void shouldCreateNewRetryableExceptionWithNullRetryAfterWhen429WithNullRetryAfterAndNotRetryableException() {
        Response response = makeDefaultDecoderReplyWhenReceivingResponse(STATUS_429, NON_RETRYABLE_EXCEPTION);
        Exception exception = atlasDbDecoder.decode(EMPTY_METHOD_KEY, response);
        assertNull(((RetryableException) exception).retryAfter());
    }

    @Test
    public void shouldCreateNewRetryableExceptionWithNullRetryAfterWhen429WithRetryAfterAndNotRetryableException() {
        Response response = makeDefaultDecoderReplyWithHeadersWhenReceivingResponse(
                STATUS_429,
                NON_RETRYABLE_EXCEPTION,
                HEADERS_WITH_RETRY_AFTER);
        Exception exception = atlasDbDecoder.decode(EMPTY_METHOD_KEY, response);
        assertNull(((RetryableException) exception).retryAfter());
    }

    @Test
    public void shouldDelegateToDefaultDecoderWhen429AndRetryableException() {
        Response response = makeDefaultDecoderReplyWhenReceivingResponse(STATUS_429, RETRYABLE_EXCEPTION);
        Exception exception = atlasDbDecoder.decode(EMPTY_METHOD_KEY, response);
        assertThat(exception, is(sameInstance(RETRYABLE_EXCEPTION)));
    }

    // 503 tests
    @Test
    public void shouldCreateNewRetryableExceptionWithMatchingNullRetryAfterWhen503AndNotRetryableException() {
        Response response = makeDefaultDecoderReplyWhenReceivingResponse(STATUS_503, NON_RETRYABLE_EXCEPTION);
        Exception exception = atlasDbDecoder.decode(EMPTY_METHOD_KEY, response);
        assertNull(((RetryableException) exception).retryAfter());
    }

    @Test
    public void shouldCreateNewRetryableExceptionWithMatchingNonnullRetryAfterWhen503AndNotRetryableException() {
        Response response = makeDefaultDecoderReplyWithHeadersWhenReceivingResponse(
                STATUS_503,
                NON_RETRYABLE_EXCEPTION,
                HEADERS_WITH_RETRY_AFTER);
        Exception exception = atlasDbDecoder.decode(EMPTY_METHOD_KEY, response);
        assertThat(((RetryableException) exception).retryAfter(), is(RETRY_AFTER_DATE));
    }

    @Test
    public void shouldDelegateToDefaultDecoderWhen503AndRetryableException() {
        Response response = makeDefaultDecoderReplyWhenReceivingResponse(STATUS_503, RETRYABLE_EXCEPTION);
        Exception exception = atlasDbDecoder.decode(EMPTY_METHOD_KEY, response);
        assertThat(exception, is(sameInstance(RETRYABLE_EXCEPTION)));
    }

    // Not 503 nor 429 tests
    @Test
    public void shouldDelegateToDefaultDecoderWhenNeither503Nor429NorRetryableException() {
        Response response = makeDefaultDecoderReplyWhenReceivingResponse(
                STATUS_NOT_503_NOR_429,
                NON_RETRYABLE_EXCEPTION);
        Exception exception = atlasDbDecoder.decode(EMPTY_METHOD_KEY, response);
        assertThat(exception, is(sameInstance(NON_RETRYABLE_EXCEPTION)));
    }

    @Test
    public void shouldDelegateToDefaultDecoderWhenNeither503Nor429AndRetryableException() {
        Response response = makeDefaultDecoderReplyWhenReceivingResponse(STATUS_NOT_503_NOR_429, RETRYABLE_EXCEPTION);
        Exception exception = atlasDbDecoder.decode(EMPTY_METHOD_KEY, response);
        assertThat(exception, is(sameInstance(RETRYABLE_EXCEPTION)));
    }

    private Response makeDefaultDecoderReplyWhenReceivingResponse(int status, Exception exception) {
        return makeDefaultDecoderReplyWithHeadersWhenReceivingResponse(status, exception, EMPTY_HEADERS);
    }

    private Response makeDefaultDecoderReplyWithHeadersWhenReceivingResponse(
            int status,
            Exception exception,
            Map<String, Collection<String>> headers) {
        Response response = createResponse(status, headers);
        when(defaultDecoder.decode(EMPTY_METHOD_KEY, response)).thenReturn(exception);
        return response;
    }

    private static Response createResponse(int status, Map<String, Collection<String>> headers) {
        return Response.create(status, EMPTY_REASON, headers, EMPTY_BODY);
    }
}
