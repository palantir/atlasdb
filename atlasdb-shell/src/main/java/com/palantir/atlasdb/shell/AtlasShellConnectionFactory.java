/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.shell;

import com.palantir.atlasdb.transaction.impl.ShellAwareReadOnlyTransactionManager;
import com.palantir.atlasdb.transaction.impl.SnapshotTransactionManager;

/**
 * The {@link AtlasShellConnectionFactory} factory, as you may have guessed, knows how to make
 * {@link AtlasShellConnection}s.
 */
public final class AtlasShellConnectionFactory {
    private final AtlasShellContextFactory atlasShellContextFactory;

    public AtlasShellConnectionFactory(AtlasShellContextFactory atlasShellContextFactory) {
        this.atlasShellContextFactory = atlasShellContextFactory;
    }

    /**
     * Create an {@link AtlasShellConnection} with a {@link ShellAwareReadOnlyTransactionManager} in
     * it
     */
    public AtlasShellConnection withReadOnlyTransactionManagerCassandra(String host, String port, String keyspace) {
        AtlasContext atlasContext = atlasShellContextFactory.withReadOnlyTransactionManagerCassandra(host, port, keyspace);
        return AtlasShellConnection.createAtlasShellConnection(atlasContext);
    }

    /**
     * Create an {@link AtlasShellConnection} with a {@link SnapshotTransactionManager} in it,
     * implemented entirely in memory and with blank initial state
     */
    public AtlasShellConnection withSnapshotTransactionManagerInMemory() {
        AtlasContext atlasContext = atlasShellContextFactory.withSnapshotTransactionManagerInMemory();
        return AtlasShellConnection.createAtlasShellConnection(atlasContext);
    }
}
