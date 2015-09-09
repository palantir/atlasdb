package com.palantir.atlasdb.keyvalue.remoting;

import java.lang.reflect.Type;

import feign.RequestTemplate;
import feign.codec.EncodeException;
import feign.codec.Encoder;

public class OctetStreamDelegateEncoder implements Encoder {

    final Encoder delegate;

    public OctetStreamDelegateEncoder(Encoder delegate) {
        this.delegate = delegate;
    }

    @Override
    public void encode(Object object, Type bodyType, RequestTemplate template)
            throws EncodeException {
        if (object instanceof byte[]) {
            template.body((byte[]) object, null);
        } else {
            delegate.encode(object, bodyType, template);
        }
    }

}
