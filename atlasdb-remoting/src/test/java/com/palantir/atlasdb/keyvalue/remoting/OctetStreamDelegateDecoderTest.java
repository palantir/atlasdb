/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.remoting;

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

public class OctetStreamDelegateDecoderTest {
    private static final int HTTP_OK = 200;
    private static final String REASON = "reason";

    private final Decoder delegate = mock(Decoder.class);
    private final Decoder decoder = new OctetStreamDelegateDecoder(delegate);

    @Test
    public void decodesApplicationOctetStreamContent() throws IOException {
        Map<String, Collection<String>> headerMap = ImmutableMap.of(
                HttpHeaders.CONTENT_TYPE, ImmutableList.of(MediaType.APPLICATION_OCTET_STREAM));
        verifyNotDelegated(headerMap);
    }

    @Test
    public void delegatesNonApplicationOctetStreamContent() throws IOException {
        Map<String, Collection<String>> headerMap = ImmutableMap.of(
                HttpHeaders.CONTENT_TYPE, ImmutableList.of(MediaType.TEXT_PLAIN));
        verifyDelegated(headerMap);
    }

    @Test
    public void delegatesContentWithNoContentTypeHeaders() throws IOException {
        Map<String, Collection<String>> headerMap = ImmutableMap.of(
                HttpHeaders.ACCEPT, ImmutableList.of(MediaType.APPLICATION_OCTET_STREAM));
        verifyDelegated(headerMap);
    }

    private Response createResponse(Map<String, Collection<String>> headerMap) {
        return Response.create(HTTP_OK, REASON, headerMap, mock(Response.Body.class));
    }

    private void verifyNotDelegated(Map<String, Collection<String>> headerMap) throws IOException {
        decoder.decode(createResponse(headerMap), mock(Type.class));
        verify(delegate, never()).decode(any(), any());
    }

    private void verifyDelegated(Map<String, Collection<String>> headerMap) throws IOException {
        decoder.decode(createResponse(headerMap), mock(Type.class));
        verify(delegate).decode(any(), any());
    }
}
