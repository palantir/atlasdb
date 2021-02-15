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
package com.palantir.util.streams;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Don't break the API
public final class PTStreams {
    private PTStreams() {
        // utility class
    }

    /**
     * Copy an input stream to an output stream using a 64k buffer.
     */
    public static long copy(InputStream input, OutputStream output) throws IOException {
        byte[] buffer = new byte[1 << 16]; // 64k
        return copy(input, output, buffer);
    }

    /**
     * Copy an input stream to an output stream using the buffer.
     */
    public static long copy(InputStream input, OutputStream output, byte[] buffer) throws IOException {
        long count = 0;
        int bytesRead = 0;
        while (-1 != (bytesRead = input.read(buffer))) {
            output.write(buffer, 0, bytesRead);
            count += bytesRead;
        }
        return count;
    }

    public static byte[] toByteArray(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        copy(input, output);
        return output.toByteArray();
    }

    public static boolean equals(InputStream stream1, InputStream stream2) throws IOException {
        DataInputStream s2 = new DataInputStream(stream2);
        try {
            byte[] array1 = new byte[1 << 16];
            byte[] array2 = new byte[1 << 16];
            while (true) {
                int bytes = stream1.read(array1);
                if (bytes < 0) {
                    return s2.read() < 0; // also end of stream2
                }

                s2.readFully(array2, 0, bytes); // throws EOFException if it can't read the bytes
                for (int i = 0; i < bytes; i++) {
                    if (array1[i] != array2[i]) {
                        return false;
                    }
                }
            }
        } catch (EOFException e) {
            return false;
        } finally {
            stream1.close();
            stream2.close();
        }
    }
}
