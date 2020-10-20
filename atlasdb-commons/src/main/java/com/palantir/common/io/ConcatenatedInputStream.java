/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.common.io;

import com.google.common.collect.Queues;
import java.io.IOException;
import java.io.InputStream;
import java.util.Queue;

public class ConcatenatedInputStream extends InputStream {
    private final Queue<InputStream> streams;

    public ConcatenatedInputStream(Iterable<InputStream> streams) {
        this.streams = Queues.newArrayDeque(streams);
    }

    @Override
    public int read() throws IOException {
        if (streams.isEmpty()) {
            return -1;
        }
        InputStream stream = streams.peek();
        int read = stream.read();
        if (read == -1) {
            stream.close();
            streams.poll();
            return read();
        }
        return read;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        if (streams.isEmpty()) {
            return -1;
        }
        InputStream stream = streams.peek();
        int read = stream.read(b, off, len);
        if (read == -1) {
            stream.close();
            streams.poll();
            return read(b, off, len);
        }
        return read;
    }

    @Override
    public void close() throws IOException {
        for (InputStream stream = streams.poll(); stream != null; stream = streams.poll()) {
            stream.close();
        }
    }
}
