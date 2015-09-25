package com.palantir.atlasdb.keyvalue.remoting;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import com.palantir.atlasdb.keyvalue.partition.exception.EndpointVersionTooOldException;

public class EndpointVersionTooOldExceptionMapper implements ExceptionMapper<EndpointVersionTooOldException> {

    private static final EndpointVersionTooOldExceptionMapper instance = new EndpointVersionTooOldExceptionMapper();
    public static EndpointVersionTooOldExceptionMapper instance() {
        return instance;
    }

    @Override
    public Response toResponse(EndpointVersionTooOldException exception) {
        return Response.noContent().status(406).build();
    }

}
