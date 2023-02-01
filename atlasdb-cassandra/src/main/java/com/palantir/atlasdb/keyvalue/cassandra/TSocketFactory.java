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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.net.Socket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

interface TSocketFactory {
    TSocket create(Socket socket) throws TTransportException;

    TSocket create(String host, int port, int timeout) throws TTransportException;

    enum Default implements TSocketFactory {
        INSTANCE;

        @Override
        public TSocket create(Socket socket) throws TTransportException {
            return new TSocket(socket);
        }

        @Override
        public TSocket create(String host, int port, int timeout) throws TTransportException {
            return new TSocket(host, port, timeout);
        }
    }
}
