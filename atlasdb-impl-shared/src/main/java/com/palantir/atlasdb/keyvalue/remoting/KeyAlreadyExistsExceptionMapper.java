package com.palantir.atlasdb.keyvalue.remoting;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;

public class KeyAlreadyExistsExceptionMapper implements ExceptionMapper<KeyAlreadyExistsException> {
    private final static KeyAlreadyExistsExceptionMapper instance = new KeyAlreadyExistsExceptionMapper();
    private KeyAlreadyExistsExceptionMapper() { }

    public static KeyAlreadyExistsExceptionMapper instance() {
        return instance;
    }

    @Override
    public Response toResponse(KeyAlreadyExistsException exception) {
        return Response
        		.status(409)
        		.entity("Key already exists!")
        		.build();
    }

}
