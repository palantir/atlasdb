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
package com.palantir.common.base;

import java.util.List;

public interface BatchingVisitable<T> {
    /**
     * This method should be used to visit elements in batches until the visitor returns false
     * or there are no batches left to visit.
     *
     * @param batchSize Each list passed to the visitor will be of batchSize except for the last
     *                  one which could be smaller, but will not be empty;
     * @return true if the visitor always returned true or was never called. false if the visitor ever returned false.
     */
    <K extends Exception> boolean batchAccept(int batchSize, AbortingVisitor<? super List<T>, K> v) throws K;
}
