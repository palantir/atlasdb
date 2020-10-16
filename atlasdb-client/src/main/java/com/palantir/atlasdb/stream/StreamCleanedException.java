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
package com.palantir.atlasdb.stream;

/**
 * This may be thrown if trying to mark a stream that no longer exists because it was cleaned up due to having no
 * references.
 *
 * @author carrino
 */
public class StreamCleanedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public StreamCleanedException(String message, Throwable cause) {
        super(message, cause);
    }

    public StreamCleanedException(String message) {
        super(message);
    }

    public StreamCleanedException(Throwable cause) {
        super(cause);
    }
}
