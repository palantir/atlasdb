/**
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import feign.Response;
import feign.RetryableException;
import feign.codec.ErrorDecoder;

public class AtlasDbErrorDecoder implements ErrorDecoder {
    private static final Logger log = LoggerFactory.getLogger(AtlasDbErrorDecoder.class);
    private ErrorDecoder defaultErrorDecoder = new ErrorDecoder.Default();

    public AtlasDbErrorDecoder() {
    }

    @VisibleForTesting
    AtlasDbErrorDecoder(ErrorDecoder errorDecoder) {
        defaultErrorDecoder = errorDecoder;
    }

    @Override
    public Exception decode(String methodKey, Response response) {
        Exception exception = defaultErrorDecoder.decode(methodKey, response);
        log.error("Decode({}, {}) yields......", methodKey, response, exception);
        if (response503ButExceptionIsNotRetryable(response, exception)) {
            return new PotentialFollowerException(exception.getMessage(), exception, null);
        }
        return exception;
    }

    private boolean response503ButExceptionIsNotRetryable(Response response, Exception exception) {
        boolean responseIs503 = response.status() == 503;
        boolean isRetryableException = exception instanceof RetryableException;
        return responseIs503 && !isRetryableException;
    }
}
