package com.palantir.server;

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
    ObjectMapper mapper = new ObjectMapper();
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
            template.header("SERVICE_CONEXT_OUT_OF_BAND", headerString);
        } catch (JsonProcessingException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }
}