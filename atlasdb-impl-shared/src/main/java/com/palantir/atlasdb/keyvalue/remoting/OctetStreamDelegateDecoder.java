/**
 * Copyright 2015 Palantir Technologies
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

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import com.google.common.collect.Iterables;

import feign.FeignException;
import feign.codec.DecodeException;
import feign.codec.Decoder;

public final class OctetStreamDelegateDecoder implements Decoder {
    private final Decoder delegate;

    public OctetStreamDelegateDecoder(Decoder delegate) {
        this.delegate = delegate;
    }

    @Override
    public Object decode(feign.Response response, Type type) throws IOException, DecodeException, FeignException {
        Collection<String> contentTypes = response.headers().get(HttpHeaders.CONTENT_TYPE);
        if (contentTypes != null
                && contentTypes.size() == 1
                && Iterables.getOnlyElement(contentTypes, "").equals(MediaType.APPLICATION_OCTET_STREAM)) {
            if (response.body() == null || response.body().length() == null) {
                return null;
            }
            byte[] data = new byte[response.body().length()];
            int bytesRead = 0;
            int bytesLeft = response.body().length();
            while (bytesLeft > 0) {
                int ret = response.body().asInputStream().read(data, bytesRead, bytesLeft);
                if (ret < 0) {
                    throw new RuntimeException("Unexpected end of stream");
                }
                bytesLeft -= ret;
                bytesRead += ret;
            }
            return data;
        }
        return delegate.decode(response, type);
    }
}
