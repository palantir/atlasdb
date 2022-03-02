/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.nexus.db.pool;

import com.google.common.reflect.Reflection;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ConnectionWithCallback {
    private ConnectionWithCallback() {
        // why
    }

    /**
     * Returns a proxy that delegates all calls, and calls the callback exactly once immediately after the
     * {@link Connection#close()} method is called.
     */
    @SuppressWarnings("ProxyNonConstantType")
    public static Connection wrap(Connection delegate, Runnable callback) {
        AtomicBoolean runCallback = new AtomicBoolean(true);
        return Reflection.newProxy(Connection.class, (proxy, method, args) -> {
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            } finally {
                if (method.getName().equals("close")) {
                    if (runCallback.compareAndExchange(true, false)) {
                        callback.run();
                    }
                }
            }
        });
    }
}
