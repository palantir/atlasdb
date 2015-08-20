package com.palantir.atlasdb.keyvalue.remoting;

import java.util.Objects;

import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;

import feign.Response;
import feign.codec.ErrorDecoder;

public class KeyValueServiceErrorDecoder implements ErrorDecoder {

    private static final ErrorDecoder defaultDecoder = new ErrorDecoder.Default();

    @Override
    public Exception decode(String methodKey, Response response) {
        if (response != null) {
            if (response.status() == 409) {
                return new KeyAlreadyExistsException(Objects.toString(response.body()));
            }
            if (response.status() == 503) {
                return new InsufficientConsistencyException(Objects.toString(response.body()));
            }
        }
        return defaultDecoder.decode(methodKey, response);
    }

}
