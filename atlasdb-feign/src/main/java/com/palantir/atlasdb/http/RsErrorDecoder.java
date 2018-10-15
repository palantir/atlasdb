/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.http;

import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotAcceptableException;
import javax.ws.rs.NotAllowedException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.NotSupportedException;
import javax.ws.rs.RedirectionException;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.ServiceUnavailableException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.google.common.io.CharStreams;

import feign.codec.ErrorDecoder;

class RsErrorDecoder implements ErrorDecoder {
    @Override
    public RuntimeException decode(String methodKey, feign.Response feignResponse) {
        try {
            Response response = convertResponseToRs(feignResponse);

            int statusCode = response.getStatus();
            Response.Status status = Response.Status.fromStatusCode(statusCode);

            if (status == null) {
                Response.Status.Family statusFamily = response.getStatusInfo().getFamily();
                return createExceptionForFamily(response, statusFamily);
            } else {
                switch (status) {
                    case BAD_REQUEST:
                        return new BadRequestException(response);
                    case UNAUTHORIZED:
                        return new NotAuthorizedException(response);
                    case FORBIDDEN:
                        return new ForbiddenException(response);
                    case NOT_FOUND:
                        return new NotFoundException(response);
                    case METHOD_NOT_ALLOWED:
                        return new NotAllowedException(response);
                    case NOT_ACCEPTABLE:
                        return new NotAcceptableException(response);
                    case UNSUPPORTED_MEDIA_TYPE:
                        return new NotSupportedException(response);
                    case INTERNAL_SERVER_ERROR:
                        return new InternalServerErrorException(response);
                    case SERVICE_UNAVAILABLE:
                        return new ServiceUnavailableException(response);
                    default:
                        Response.Status.Family statusFamily = response.getStatusInfo().getFamily();
                        return createExceptionForFamily(response, statusFamily);
                }
            }
        } catch (Throwable t) {
            return new RuntimeException("Failed to convert response to exception", t);
        }
    }

    private Response convertResponseToRs(feign.Response feignResponse) throws IOException {
        Response.ResponseBuilder responseBuilder = Response.status(feignResponse.status());
        if (feignResponse.body() != null) {
            String body = CharStreams.toString(feignResponse.body().asReader());
            responseBuilder.entity(body);
            for (Entry<String, Collection<String>> header : feignResponse.headers().entrySet()) {
                for (String headerValue : header.getValue()) {
                    responseBuilder.header(header.getKey(), headerValue);
                }
            }
        }
        return responseBuilder.build();
    }

    private WebApplicationException createExceptionForFamily(Response response, Response.Status.Family statusFamily) {
        switch (statusFamily) {
            case REDIRECTION:
                return new RedirectionException(response);
            case CLIENT_ERROR:
                return new ClientErrorException(response);
            case SERVER_ERROR:
                return new ServerErrorException(response);
            default:
                return new WebApplicationException(response);
        }
    }
}
