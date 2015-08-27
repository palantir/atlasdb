package com.palantir.atlasdb.keyvalue.remoting;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import com.palantir.atlasdb.keyvalue.partition.exception.VersionTooOldException;


public class VersionTooOldExceptionMapper implements ExceptionMapper<VersionTooOldException> {

    private VersionTooOldExceptionMapper() {}
    private static VersionTooOldExceptionMapper instance = new VersionTooOldExceptionMapper();
    public static VersionTooOldExceptionMapper instance() {
        return instance;
    }

    @Override
    public Response toResponse(VersionTooOldException exception) {
        return Response.noContent().status(410).build();
    }

}
