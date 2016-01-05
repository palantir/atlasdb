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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.net.InetAddress;

import com.google.common.base.Function;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.pooling.PoolingContainer;

public interface ManyHostPoolingContainer<T> extends PoolingContainer<T> {
    <V, K extends Exception> V runWithPooledResourceOnHost(InetAddress host,
                                                           FunctionCheckedException<T, V, K> f) throws K;

    <V> V runWithPooledResourceOnHost(InetAddress host, Function<T, V> f);
}
