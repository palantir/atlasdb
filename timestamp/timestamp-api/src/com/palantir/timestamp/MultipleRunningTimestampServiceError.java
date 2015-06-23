// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.timestamp;


/**
 * Error indicating that multiple timestamp services are running against the
 * same database.
 */
public class MultipleRunningTimestampServiceError extends Error {
    private static final long serialVersionUID = 1L;

    public MultipleRunningTimestampServiceError(String message) {
        super(message);
    }

    public MultipleRunningTimestampServiceError(String message, Throwable t) {
        super(message, t);
    }

    public MultipleRunningTimestampServiceError(Throwable t) {
        super(t);
    }
}
