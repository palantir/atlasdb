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
package com.palantir.atlasdb.keyvalue.remoting.outofband;

import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.palantir.common.base.Throwables;
import com.palantir.common.supplier.RemoteContextHolder;
import com.palantir.common.supplier.RemoteContextHolder.RemoteContextType;

import feign.RequestInterceptor;
import feign.RequestTemplate;

public class OutboxShippingInterceptor implements RequestInterceptor {
    final ObjectMapper mapper;
    public static final String CONTEXT_HEADER_NAME = "SERVICE_CONTEXT_OUT_OF_BAND";

    public OutboxShippingInterceptor(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void apply(RequestTemplate template) {
        Map<RemoteContextType<?>, Object> map = RemoteContextHolder.OUTBOX.getHolderContext().get();
        Map<String, Object> toSerialize = Maps.newHashMap();
        for (Entry<RemoteContextType<?>, Object> e : map.entrySet()) {
            Enum<?> key = (Enum<?>) e.getKey();
            String keyStr = key.getDeclaringClass().getName() + "." + key.name();
            toSerialize.put(keyStr, e.getValue());
        }
        try {
            String headerString = mapper.writeValueAsString(toSerialize);
            template.header(CONTEXT_HEADER_NAME, headerString);
        } catch (JsonProcessingException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }
}