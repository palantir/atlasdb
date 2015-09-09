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

final class OctetStreamDelegateDecoder implements Decoder {
    private final Decoder delegate;

    public OctetStreamDelegateDecoder(Decoder delegate) {
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