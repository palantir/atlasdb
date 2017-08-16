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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.common.base.FunctionCheckedException;
import java.lang.Exception;
import java.lang.Override;
import java.net.InetSocketAddress;
import org.apache.cassandra.thrift.Cassandra;

public abstract class AutoDelegate_CassandraClientPool extends CassandraClientPool {

  public AutoDelegate_CassandraClientPool(CassandraKeyValueServiceConfig config) {
    super(config);
  }

  public abstract CassandraClientPool delegate();

  @Override
  public <V, K extends Exception> V run(FunctionCheckedException<Cassandra.Client, V, K> fn) throws
      K {
    return delegate().run(fn);
  }

  @Override
  public void runOneTimeStartupChecks() {
    delegate().runOneTimeStartupChecks();
  }

  @Override
  public <V, K extends Exception> V runWithRetry(FunctionCheckedException<Cassandra.Client, V, K> fn)
      throws K {
    return delegate().runWithRetry(fn);
  }

  @Override
  public InetSocketAddress getRandomHostForKey(byte[] key) {
    return delegate().getRandomHostForKey(key);
  }

  @Override
  public void shutdown() {
    delegate().shutdown();
  }

  @Override
  public <V, K extends Exception> V runWithRetryOnHost(InetSocketAddress specifiedHost,
      FunctionCheckedException<Cassandra.Client, V, K> fn) throws K {
    return delegate().runWithRetryOnHost(specifiedHost,fn);
  }

  @Override
  public void tryInitialize() {
    delegate().tryInitialize();
  }
}
