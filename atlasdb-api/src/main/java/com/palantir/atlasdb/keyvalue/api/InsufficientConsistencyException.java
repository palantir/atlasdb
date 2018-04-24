/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.api;

import com.palantir.common.exception.AtlasDbDependencyException;

/**
 * Thrown by a key value service when an operation could not be performed
 * because the required consistency could not be met.
 */
public class InsufficientConsistencyException extends AtlasDbDependencyException {
    private static final long serialVersionUID = 1L;

    public InsufficientConsistencyException(String msg) {
        super(msg);
    }

    public InsufficientConsistencyException(String msg, Throwable ex) {
        super(msg, ex);
    }
}
