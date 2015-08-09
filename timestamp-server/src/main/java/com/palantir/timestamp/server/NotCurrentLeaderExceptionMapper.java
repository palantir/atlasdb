package com.palantir.timestamp.server;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import com.palantir.leader.NotCurrentLeaderException;

/**
 * Convert {@link NotCurrentLeaderException} into a 503 status response.
 *
 * @author carrino
 */
public class NotCurrentLeaderExceptionMapper implements ExceptionMapper<NotCurrentLeaderException> {
    @Override
    public Response toResponse(NotCurrentLeaderException exception) {
        return Response.noContent().status(503)
                .header(feign.Util.RETRY_AFTER, "0")
                .build();
    }
}
