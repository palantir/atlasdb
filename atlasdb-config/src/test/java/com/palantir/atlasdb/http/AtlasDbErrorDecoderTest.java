/**
 * Copyright 2016 Palantir Technologies
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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import feign.Response;
import feign.RetryableException;
import feign.codec.ErrorDecoder;

public class AtlasDbErrorDecoderTest {
    private static final String EMPTY_METHOD_KEY = "";

    private static final Exception NON_RETRYABLE_EXCEPTION = new Exception();
    private static final Exception RETRYABLE_EXCEPTION = new RetryableException("MyException", new Date());

    private static final int STATUS_503 = 503;
    private static final int STATUS_NOT_503 = 511;

    ErrorDecoder defaultDecoder;
    AtlasDbErrorDecoder atlasDbDecoder;

    @Before
    public void setUp() {
        defaultDecoder = mock(ErrorDecoder.class);
        atlasDbDecoder = new AtlasDbErrorDecoder(defaultDecoder);
    }

    @Test
    public void shouldCreateNewRetryableExceptionWithNullRetryAfterWhen503AndNotRetryableException() {
        Response response = makeDefaultDecoderReplyWhenReceivingResponse(STATUS_503, NON_RETRYABLE_EXCEPTION);

        Exception exception = atlasDbDecoder.decode(EMPTY_METHOD_KEY, response);

        assertNull(((RetryableException) exception).retryAfter());
    }

    @Test
    public void shouldDelegateToDefaultDecoderWhen503AndRetryableException() {
        Response response = makeDefaultDecoderReplyWhenReceivingResponse(STATUS_503, RETRYABLE_EXCEPTION);

        Exception exception = atlasDbDecoder.decode(EMPTY_METHOD_KEY, response);

        assertThat(exception, is(sameInstance(RETRYABLE_EXCEPTION)));
    }

    @Test
    public void shouldDelegateToDefaultDecoderWhenNeither503NorRetryableException() {
        Response response = makeDefaultDecoderReplyWhenReceivingResponse(STATUS_NOT_503, NON_RETRYABLE_EXCEPTION);

        Exception exception = atlasDbDecoder.decode(EMPTY_METHOD_KEY, response);

        assertThat(exception, is(sameInstance(NON_RETRYABLE_EXCEPTION)));
    }

    @Test
    public void shouldDelegateToDefaultDecoderWhenNot503AndRetryableException() {
        Response response = makeDefaultDecoderReplyWhenReceivingResponse(STATUS_NOT_503, RETRYABLE_EXCEPTION);

        Exception exception = atlasDbDecoder.decode(EMPTY_METHOD_KEY, response);

        assertThat(exception, is(sameInstance(RETRYABLE_EXCEPTION)));
    }

    private Response makeDefaultDecoderReplyWhenReceivingResponse(int status, Exception exception) {
        Response response = createResponse(status);
        when(defaultDecoder.decode(EMPTY_METHOD_KEY, response))
                .thenReturn(exception);
        return response;
    }

    private static Response createResponse(int status) {
        Map<String, Collection<String>> emptyHeaders = new HashMap<>();
        String emptyReason = "";
        byte[] emptyBody = new byte[0];
        return Response.create(status, emptyReason, emptyHeaders, emptyBody);
    }
}