/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.timestamp;

/**
 * This class is used to create persistent timestamp services.
 * @author carrino
 *
 */
public interface TimestampBoundStore {
    /**
     * This will be called when the timestamp server is first created.
     * <p>
     * This is guarenteed to be called before the first call to {@link #storeUpperLimit(long)}
     *
     * @return the current timestamp upper limit that is persisted
     */
    long getUpperLimit();

    /**
     * Persists a new timestamp upper limit.
     * <p>
     * This method must be atomic and ensure that the value has not changed since the last
     * call to {@link #storeUpperLimit(long)} or {@link #getUpperLimit()}
     * <p>
     * If the limit has changed out from under us then this method MUST throw MultipleRunningTimestampServiceException.
     *
     * @param limit the new upper limit to be stored
     * @throws MultipleRunningTimestampServiceException if the timestamp has changed out from under us.
     */
    void storeUpperLimit(long limit) throws MultipleRunningTimestampServiceException;
}
