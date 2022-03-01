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
import java.sql.Connection;

public final class ConnectionWithCallback {
    private ConnectionWithCallback() {
        // sdfgj
    }

    @SuppressWarnings("ProxyNonConstantType")
    public static Connection wrap(Connection delegate, Runnable callback) {
        return Reflection.newProxy(Connection.class, (proxy, method, args) -> {
            try {
                return method.invoke(delegate, args);
            } finally {
                if (method.getName().equals("close")) {
                    callback.run();
                }
            }
        });
    }
}
