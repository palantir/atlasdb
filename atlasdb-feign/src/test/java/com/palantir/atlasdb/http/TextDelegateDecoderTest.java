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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HttpHeaders;

import feign.Response;
import feign.codec.Decoder;

public class TextDelegateDecoderTest {
    private static final int HTTP_OK = 200;
    private static final String REASON = "reason";
    private static final String CONTENT_TYPE_ALT_CASING_1 = "content-type";
    private static final String CONTENT_TYPE_ALT_CASING_2 = "COnTeNt-tYPe";

    private final Decoder delegate = mock(Decoder.class);
    private final TextDelegateDecoder textDelegateDecoder = new TextDelegateDecoder(delegate);

    @Test
    public void delegatesContentWithNoHttpHeaders() throws IOException {
        Response response = createResponse(ImmutableMap.of());
        verifyDelegated(response);
    }

    @Test
    public void delegatesApplicationJsonContent() throws IOException {
        Response response = createSingleHeaderResponse(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
        verifyDelegated(response);
    }

    @Test
    public void decodesTextPlainContent() throws IOException {
        Response response = createSingleHeaderResponse(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_PLAIN);
        verifyNotDelegated(response);
    }

    @Test
    public void decodesTextPlainContentRegardlessOfCase() throws IOException {
        Response response1 = createSingleHeaderResponse(CONTENT_TYPE_ALT_CASING_1, MediaType.TEXT_PLAIN);
        verifyNotDelegated(response1);

        Response response2 = createSingleHeaderResponse(CONTENT_TYPE_ALT_CASING_2, MediaType.TEXT_PLAIN);
        verifyNotDelegated(response2);
    }

    @Test
    public void delegatesContentWithOtherTextPlainHttpHeaders() throws IOException {
        Response response = createResponse(
                ImmutableMap.<String, Collection<String>>builder()
                        .put(HttpHeaders.ACCEPT, ImmutableList.of(MediaType.TEXT_PLAIN))
                        .put(HttpHeaders.CONTENT_TYPE, ImmutableList.of(MediaType.APPLICATION_JSON))
                        .build());

        verifyDelegated(response);
    }

    @Test
    public void decodesContentWithContentTypeTextPlainRegardlessOfOtherHeaders() throws IOException {
        Response response = createResponse(
                ImmutableMap.<String, Collection<String>>builder()
                        .put(HttpHeaders.CONTENT_TYPE, ImmutableList.of(MediaType.TEXT_PLAIN))
                        .put(HttpHeaders.ACCEPT, ImmutableList.of(MediaType.APPLICATION_JSON))
                        .build());

        verifyNotDelegated(response);
    }

    private Response createResponse(Map<String, Collection<String>> headerMap) {
        return Response.create(HTTP_OK, REASON, headerMap, mock(Response.Body.class));
    }

    private Response createSingleHeaderResponse(String header, String... values) {
        return createResponse(ImmutableMap.of(header, ImmutableList.copyOf(values)));
    }

    private void verifyDelegated(Response response) throws IOException {
        textDelegateDecoder.decode(response, mock(Type.class));
        verify(delegate).decode(any(), any());
    }

    private void verifyNotDelegated(Response response) throws IOException {
        textDelegateDecoder.decode(response, String.class);
        verify(delegate, never()).decode(any(), any());
    }
}
