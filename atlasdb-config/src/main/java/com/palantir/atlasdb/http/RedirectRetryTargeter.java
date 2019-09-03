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

import java.net.MalformedURLException;
import java.net.URL;

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public class RedirectRetryTargeter {
    private final URL localServerBaseUrl;
    private final URL nextServerBaseUrl;

    public RedirectRetryTargeter(URL localServerBaseUrl, URL nextServerBaseUrl) {
        this.localServerBaseUrl = localServerBaseUrl;
        this.nextServerBaseUrl = nextServerBaseUrl;
    }

    // Precondition: requestUrl is a suffix of the local server's base URL.
    public URL redirectRequest(URL requestUrl) {
        String requestContextPath = getRequestContextPath(requestUrl);
        try {
            return new URL(
                    nextServerBaseUrl.getProtocol(),
                    nextServerBaseUrl.getHost(),
                    nextServerBaseUrl.getPort(),
                    nextServerBaseUrl.getFile() + requestContextPath);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Error when constructing a URL in RedirectRetryTargeter. This is"
                    + " a product bug. The path of this URL was " + nextServerBaseUrl.getFile() + requestContextPath);
        }
    }

    private String getRequestContextPath(URL requestUrl) {
        String localServerContextPath = localServerBaseUrl.getPath();
        String requestPath = requestUrl.getPath();
        Preconditions.checkState(requestPath.startsWith(localServerContextPath),
                "We attempted to process a request in an application-specific exception mapper that is not"
                        + " in our context path. This is strange, and a product bug.",
                SafeArg.of("localServerBaseUrl", localServerBaseUrl),
                UnsafeArg.of("requestUrl", requestUrl)); // unsafe since this is user-provided

        return requestPath.substring(localServerContextPath.length());
    }
}
