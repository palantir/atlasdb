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

import java.util.Collection;
import java.util.Date;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.net.HttpHeaders;
import com.palantir.common.remoting.HeaderAccessUtils;

import feign.Response;
import feign.RetryableException;
import feign.codec.ErrorDecoder;

public class AtlasDbErrorDecoder implements ErrorDecoder {
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
        if (response503ButExceptionIsNotRetryable(response, exception)) {
            return new RetryableException(exception.getMessage(), exception, parseRetryAfter(response));
        }
        return exception;
    }

    private Date parseRetryAfter(Response response) {
        Collection<String> retryAfterValues = HeaderAccessUtils.shortcircuitingCaseInsensitiveGet(
                response.headers(), HttpHeaders.RETRY_AFTER);
        if (retryAfterValues.isEmpty()) {
            return null;
        }
        String retryAfterValue = Iterables.getOnlyElement(retryAfterValues);
        return new Date(Long.parseLong(retryAfterValue));
    }

    private boolean response503ButExceptionIsNotRetryable(Response response, Exception exception) {
        boolean responseIs503 = response.status() == 503;
        boolean isRetryableException = exception instanceof RetryableException;
        return responseIs503 && !isRetryableException;
    }
}
