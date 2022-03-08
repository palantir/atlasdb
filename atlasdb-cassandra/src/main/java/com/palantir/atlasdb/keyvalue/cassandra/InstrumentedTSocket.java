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

import com.codahale.metrics.Counter;
import com.palantir.atlasdb.util.MetricsManager;
import java.net.Socket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

/**
 * A simple wrapping implementation of TSocket which on all reads/writes, marks the number of bytes read/written in
 * counters. This extends TSocket because TSocket has simple read methods rather than needing to override methods in
 * e.g. InputStream or OutputStream. It ignores the 'buffer' related methods because the superclasses do not set it
 * and it's optional functionality.
 */
public final class InstrumentedTSocket extends TSocket {
    private final Counter bytesRead;
    private final Counter bytesWritten;

    private InstrumentedTSocket(Socket socket, Counter bytesRead, Counter bytesWritten) throws TTransportException {
        super(socket);
        this.bytesRead = bytesRead;
        this.bytesWritten = bytesWritten;
    }

    private InstrumentedTSocket(String host, int port, int timeout, Counter bytesRead, Counter bytesWritten) {
        super(host, port, timeout);
        this.bytesRead = bytesRead;
        this.bytesWritten = bytesWritten;
    }

    @Override
    public int read(byte[] buf, int off, int len) throws TTransportException {
        int read = super.read(buf, off, len);
        bytesRead.inc(read);
        return read;
    }

    @Override
    public int readAll(byte[] buf, int off, int len) throws TTransportException {
        int read = super.readAll(buf, off, len);
        bytesRead.inc(read);
        return read;
    }

    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException {
        super.write(buf, off, len);
        bytesWritten.inc(len);
    }

    @Override
    public void write(byte[] buf) throws TTransportException {
        super.write(buf);
        bytesWritten.inc(buf.length);
    }

    public static final class Factory implements TSocketFactory {
        private final Counter bytesRead;
        private final Counter bytesWritten;

        public Factory(MetricsManager metrics) {
            this.bytesRead = metrics.registerOrGetCounter(InstrumentedTSocket.class, "bytesRead");
            this.bytesWritten = metrics.registerOrGetCounter(InstrumentedTSocket.class, "bytesWritten");
        }

        @Override
        public TSocket create(Socket socket) throws TTransportException {
            return new InstrumentedTSocket(socket, bytesRead, bytesWritten);
        }

        @Override
        public TSocket create(String host, int port, int timeout) {
            return new InstrumentedTSocket(host, port, timeout, bytesRead, bytesWritten);
        }
    }
}
