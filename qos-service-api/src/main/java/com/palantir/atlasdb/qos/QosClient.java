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

package com.palantir.atlasdb.qos;

public interface QosClient {

    interface Query<T, E extends Exception> {
        T execute() throws E;
    }

    interface QueryWeigher<T> {
        QueryWeight estimate();
        QueryWeight weighSuccess(T result, long timeTakenNanos);
        QueryWeight weighFailure(Exception error, long timeTakenNanos);
    }

    <T, E extends Exception> T executeRead(
            Query<T, E> query,
            QueryWeigher<T> weigher) throws E;

    <T, E extends Exception> T executeWrite(
            Query<T, E> query,
            QueryWeigher<T> weigher) throws E;

}
