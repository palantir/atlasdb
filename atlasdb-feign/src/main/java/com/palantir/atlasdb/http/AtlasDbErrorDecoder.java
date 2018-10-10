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

import java.util.Collection;
import java.util.Date;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;
import com.palantir.common.remoting.HeaderAccessUtils;

import feign.Response;
import feign.RetryableException;
import feign.codec.ErrorDecoder;

public class AtlasDbErrorDecoder implements ErrorDecoder {
    private ErrorDecoder delegateDecoder = new SerializableErrorDecoder();

    public AtlasDbErrorDecoder() {
    }

    @VisibleForTesting
    AtlasDbErrorDecoder(ErrorDecoder errorDecoder) {
        delegateDecoder = errorDecoder;
    }

    @Override
    public Exception decode(String methodKey, Response response) {
        Exception exception = delegateDecoder.decode(methodKey, response);
        if (response503ButExceptionIsNotRetryable(response, exception)) {
            return new RetryableException(exception.getMessage(), exception, parseRetryAfter(response));
        }
        if (response429ButExceptionIsNotRetryable(response, exception)) {
            // We want to retry on the same node (ExceptionRetryBehaviour.RETRY_ON_SAME_NODE) every time we receive
            // a response with status 429. Therefore, we set retryAfter to null to comply with
            // ExceptionRetryBehaviour.getRetryBehaviourForException logic.
            return new RetryableException(exception.getMessage(), exception, null);
        }

        return exception;
    }

    private Date parseRetryAfter(Response response) {
        Collection<String> retryAfterValues = HeaderAccessUtils.shortcircuitingCaseInsensitiveGet(
                response.headers(), HttpHeaders.RETRY_AFTER);
        if (retryAfterValues.isEmpty()) {
            return null;
        }
        String retryAfterValue = retryAfterValues.iterator().next();
        return new Date(Long.parseLong(retryAfterValue));
    }

    private boolean response503ButExceptionIsNotRetryable(Response response, Exception exception) {
        return (response.status() == 503) && !isExceptionRetryable(exception);
    }

    private boolean response429ButExceptionIsNotRetryable(Response response, Exception exception) {
        return (response.status() == 429) && !isExceptionRetryable(exception);
    }

    private boolean isExceptionRetryable(Exception exception) {
        return exception instanceof RetryableException;
    }
}
