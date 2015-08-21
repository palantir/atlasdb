package com.palantir.atlasdb.keyvalue.remoting;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;

public class InsufficientConsistencyExceptionMapper implements
        ExceptionMapper<InsufficientConsistencyException> {

    private final static InsufficientConsistencyExceptionMapper instance = new InsufficientConsistencyExceptionMapper();
    private InsufficientConsistencyExceptionMapper() { }

    public static InsufficientConsistencyExceptionMapper instance() {
        return instance;
    }

    @Override
    public Response toResponse(InsufficientConsistencyException exception) {
        // TODO: Add content (how?) with explanation
        return Response.noContent().status(503).build();
    }

}
