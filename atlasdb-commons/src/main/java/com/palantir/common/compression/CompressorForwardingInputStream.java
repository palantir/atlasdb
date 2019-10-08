/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.common.compression;

import java.io.IOException;
import java.io.InputStream;

public class CompressorForwardingInputStream extends InputStream {
    private InputStream compressedStream;
    private InputStream delegate;

    public CompressorForwardingInputStream(InputStream stream) {
        compressedStream = stream;
    }

    @Override
    public int read() throws IOException {
        if (delegate == null) {
            delegate = ClientCompressor.getDecompressorStream(compressedStream);
        }
        return delegate.read();
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (delegate != null) {
            delegate.close();
        }
    }
}
