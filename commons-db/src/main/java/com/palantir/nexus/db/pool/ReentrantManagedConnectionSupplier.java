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
package com.palantir.nexus.db.pool;

import java.sql.Connection;
import java.sql.SQLException;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.ReentrantConnectionSupplier;

@ThreadSafe
public class ReentrantManagedConnectionSupplier implements ReentrantConnectionSupplier {

    private final ConnectionManager delegate;
    private final ThreadLocal<ResourceSharer<Connection, SQLException>> threadLocal;

    public ReentrantManagedConnectionSupplier(final ConnectionManager delegate) {
        this.delegate = Preconditions.checkNotNull(delegate);
        this.threadLocal = ThreadLocal.withInitial(() ->
                new ResourceSharer<Connection, SQLException>(ResourceTypes.CONNECTION) {
                    @Override
                    public Connection open() {
                        return delegate.getConnectionUnchecked();
                    }
                });
    }

    @Override
    public Connection get() throws PalantirSqlException {
        return CloseTracking.wrap(threadLocal.get().get());
    }

    @Override
    public Connection getUnsharedConnection() throws PalantirSqlException {
        return CloseTracking.wrap(delegate.getConnectionUnchecked());
    }

    @Override
    public void close() throws PalantirSqlException {
        delegate.closeUnchecked();
    }
}
