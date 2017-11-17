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

public class FakeQosClient implements QosClient {

    public static final FakeQosClient INSTANCE = new FakeQosClient();

    @Override
    public <T, E extends Exception> T executeRead(ReadQuery<T, E> query, QueryWeigher<T> weigher)
            throws E {
        return query.execute();
    }

    @Override
    public <T, E extends Exception> void executeWrite(WriteQuery<E> query,
            QueryWeigher<Void> weigher) throws E {
        query.execute();
    }
}
