package com.palantir.atlasdb.keyvalue.remoting;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang.ArrayUtils;

import com.google.common.collect.Iterables;

import feign.FeignException;
import feign.codec.DecodeException;
import feign.codec.Decoder;

final class EmptyOctetStreamDelegateDecoder implements Decoder {
    private final Decoder delegate;

    public EmptyOctetStreamDelegateDecoder(Decoder delegate) {
        this.delegate = delegate;
    }

    @Override
    public Object decode(feign.Response response, Type type) throws IOException,
            DecodeException, FeignException {
        Collection<String> contentTypes = response.headers().get(HttpHeaders.CONTENT_TYPE);
        if (contentTypes != null
            && contentTypes.size() == 1
            && Iterables.getOnlyElement(contentTypes, "").equals(MediaType.APPLICATION_OCTET_STREAM)) {
            if (response.body() == null || response.body().length() == null) {
              return null;
            }
            if (0 == response.body().length()) {
                return ArrayUtils.EMPTY_BYTE_ARRAY;
            }
        }
        return delegate.decode(response, type);
    }
}