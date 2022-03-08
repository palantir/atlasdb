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
import java.net.Socket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

/**
 * A simple wrapping implementation of TSocket which on all reads, marks the number of bytes read in counters.
 * This extends TSocket because TSocket has simple read methods rather than needing to override methods in e.g.
 * InputStream or . It uses Counter as opposed to Meter because Counter is designed to be extremely low overhead.
 */
public final class InstrumentedSocket extends TSocket {
    private final Counter bytesRead;
    private final Counter bytesWritten;

    private InstrumentedSocket(Socket socket, Counter bytesRead, Counter bytesWritten) throws TTransportException {
        super(socket);
        this.bytesRead = bytesRead;
        this.bytesWritten = bytesWritten;
    }

    private InstrumentedSocket(String host, int port, int timeout, Counter bytesRead, Counter bytesWritten) {
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

        public Factory(Counter bytesRead, Counter bytesWritten) {
            this.bytesRead = bytesRead;
            this.bytesWritten = bytesWritten;
        }

        @Override
        public TSocket create(Socket socket) throws TTransportException {
            return new InstrumentedSocket(socket, bytesRead, bytesWritten);
        }

        @Override
        public TSocket create(String host, int port, int timeout) {
            return new InstrumentedSocket(host, port, timeout, bytesRead, bytesWritten);
        }
    }
}
