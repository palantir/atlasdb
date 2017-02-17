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
// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.http;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.net.HttpHeaders;

import feign.FeignException;
import feign.Response;
import feign.codec.Decoder;
import feign.codec.StringDecoder;

/**
 * If the response has a Content-Type of text/plain, then this decoder uses a string decoder.
 * Otherwise, it falls back to the delegate.
 * @author jmeacham
 */
public class TextDelegateDecoder implements Decoder {
    private static final Logger log = LoggerFactory.getLogger(TextDelegateDecoder.class);
    private final Decoder delegate;
    private final Decoder stringDecoder;

    public TextDelegateDecoder(Decoder delegate) {
        this.delegate = delegate;
        this.stringDecoder = new StringDecoder();
    }

    @Override
    public Object decode(Response response, Type type) throws IOException, FeignException {
        try {
            Collection<String> contentTypes = response.headers().entrySet().stream()
                    .filter(entry -> entry.getKey().compareToIgnoreCase(HttpHeaders.CONTENT_TYPE) == 0)
                    .map(Map.Entry::getValue)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());
            // In the case of multiple content types, or an unknown content type, we'll use the delegate instead.
            if (contentTypes != null
                    && contentTypes.size() == 1
                    && Iterables.getOnlyElement(contentTypes, "").equals(MediaType.TEXT_PLAIN)) {
                return stringDecoder.decode(response, type);
            }
            return delegate.decode(response, type);
        } catch (Exception e) {
            log.error("Failed decoding response for type {}: {}", type, response);
            throw e;
        }
    }
}
