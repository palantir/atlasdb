/*
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.http;

import java.io.IOException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

import feign.Response;
import feign.RetryableException;
import feign.codec.ErrorDecoder;

public class AtlasDbErrorDecoder implements ErrorDecoder {
    private static final Logger log = LoggerFactory.getLogger(AtlasDbErrorDecoder.class);
    private ErrorDecoder defaultErrorDecoder = new ErrorDecoder.Default();
    private ExceptionDecoder exceptionDecoder = ExceptionDecoder.create();

    public AtlasDbErrorDecoder() {
    }

    @VisibleForTesting
    AtlasDbErrorDecoder(ErrorDecoder errorDecoder) {
        defaultErrorDecoder = errorDecoder;
    }

    @Override
    public Exception decode(String methodKey, Response response) {
        Optional<Exception> causingExceptionOptional;
        try {
            causingExceptionOptional = exceptionDecoder.decodeCausingException(response.body().asInputStream());
        } catch (IOException e) {
            // Could not read the response body, perhaps because it was malformed.
            log.error("Encountered an error when deserializing an exception:", e);
            throw Throwables.propagate(e);
        }
        Exception feignException = defaultErrorDecoder.decode(methodKey, response);

        if (!causingExceptionOptional.isPresent()) {
            return feignException;
        }

        Exception causingException = causingExceptionOptional.get();
        if (response503ButExceptionIsNotRetryable(response, feignException)) {
            return new RetryableException(feignException.getMessage(), causingException, null);
        }
        return causingException;
    }

    private boolean response503ButExceptionIsNotRetryable(Response response, Exception exception) {
        boolean responseIs503 = response.status() == 503;
        boolean isRetryableException = exception instanceof RetryableException;
        return responseIs503 && !isRetryableException;
    }
}
