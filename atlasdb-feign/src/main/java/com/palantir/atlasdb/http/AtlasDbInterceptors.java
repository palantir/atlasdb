/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;

import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public final class AtlasDbInterceptors {
    public static final String USER_AGENT_HEADER = "User-Agent";
    public static final int MAX_PAYLOAD_SIZE = 50_000_000;

    public static final Interceptor REQUEST_PAYLOAD_LIMITER = new PayloadLimitingInterceptor();

    private AtlasDbInterceptors() {
        // utility
    }

    public static final class UserAgentAddingInterceptor implements Interceptor {
        private final String userAgent;

        public UserAgentAddingInterceptor(String userAgent) {
            Preconditions.checkNotNull(userAgent, "User Agent should never be null.");
            this.userAgent = userAgent;
        }

        @Override
        public Response intercept(Chain chain) throws IOException {
            Request requestWithUserAgent = chain.request()
                    .newBuilder()
                    .addHeader(USER_AGENT_HEADER, userAgent)
                    .build();
            return chain.proceed(requestWithUserAgent);
        }
    }

    private static final class PayloadLimitingInterceptor implements Interceptor {
        @Override
        public Response intercept(Chain chain) throws IOException {
            RequestBody body = chain.request().body();
            if (body != null && body.contentLength() >= MAX_PAYLOAD_SIZE) {
                throw new SafeIllegalArgumentException(
                        "Request too large. Maximum allowed payload size is {} bytes, but the request has {}.",
                        SafeArg.of("maximumSize", MAX_PAYLOAD_SIZE), SafeArg.of("actualSize", body.contentLength()));
            }
            return chain.proceed(chain.request());
        }
    }
}
