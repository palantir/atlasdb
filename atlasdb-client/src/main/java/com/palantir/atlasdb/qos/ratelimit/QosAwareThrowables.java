/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.qos.ratelimit;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;

import com.palantir.common.base.Throwables;
import com.palantir.conjure.java.api.errors.QosException;

public final class QosAwareThrowables {
    private QosAwareThrowables() {
        // no
    }

    /**
     * If the provided Throwable is
     *   a) an ExecutionException or InvocationTargetException, then apply this method on the cause;
     *   b) a RateLimitExceededException or an AtlasDbDependencyException, then rethrow it;
     *   c) none of the above, then wrap it in an AtlasDbDependencyException and throw that.
     */
    public static RuntimeException unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(Throwable ex) {
        if (ex instanceof ExecutionException || ex instanceof InvocationTargetException) {
            // Needs to be this way in case you have ITE(RLE) or variants of that.
            throw unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(ex.getCause());
        } else if (ex instanceof QosException.Throttle) {
            throw (QosException.Throttle) ex;
        } else if (ex instanceof com.palantir.remoting.api.errors.QosException.Throttle) {
            throw (com.palantir.remoting.api.errors.QosException.Throttle) ex;
        }
        throw Throwables.unwrapAndThrowAtlasDbDependencyException(ex);
    }

}
