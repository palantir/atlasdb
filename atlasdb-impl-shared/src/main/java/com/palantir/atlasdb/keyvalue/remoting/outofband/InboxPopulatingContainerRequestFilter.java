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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.palantir.common.base.Throwables;
import com.palantir.common.supplier.AbstractWritableServiceContext;
import com.palantir.common.supplier.RemoteContextHolder;
import com.palantir.common.supplier.RemoteContextHolder.RemoteContextType;

public class InboxPopulatingContainerRequestFilter implements ContainerRequestFilter {
    final ObjectMapper mapper;
    final ClassLoader classLoader;

    public InboxPopulatingContainerRequestFilter(ObjectMapper mapper) {
        this(mapper, null);
    }

    public InboxPopulatingContainerRequestFilter(ObjectMapper mapper, ClassLoader classLoader) {
        this.mapper = mapper;
        this.classLoader = classLoader;
    }

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        AbstractWritableServiceContext<Map<RemoteContextType<?>, Object>> holderContext = (AbstractWritableServiceContext<Map<RemoteContextType<?>, Object>>) RemoteContextHolder.INBOX.getHolderContext();
        if (!requestContext.getHeaders().containsKey(OutboxShippingInterceptor.CONTEXT_HEADER_NAME)) {
            holderContext.set(null);
            return;
        }
        Map<RemoteContextType<?>, Object> map = Maps.newHashMap();
        String header = requestContext.getHeaders().get(
                OutboxShippingInterceptor.CONTEXT_HEADER_NAME).iterator().next();
        JsonNode node = mapper.readTree(header);
        Iterator<String> it = node.fieldNames();
        while (it.hasNext()) {
            String key = it.next();
            int lastIndex = key.lastIndexOf(".");
            String enumName = key.substring(lastIndex + 1, key.length());
            String className = key.substring(0, lastIndex);
            try {
                @SuppressWarnings({ "rawtypes", "unchecked" })
                Class<Enum> clazz = (Class<Enum>) Class.forName(className, true,
                        classLoader == null ? Thread.currentThread().getContextClassLoader() : classLoader);
                @SuppressWarnings({ "rawtypes", "unchecked" })
                Enum enumValue = Enum.valueOf(clazz, enumName);
                RemoteContextType<?> remoteType = (RemoteContextType<?>) enumValue;
                Object value = mapper.treeToValue(node.get(key), remoteType.getValueType());
                map.put(remoteType, value);
            } catch (ClassNotFoundException e) {
                throw Throwables.throwUncheckedException(e);
            }
        }
        holderContext.set(map);
    }
}