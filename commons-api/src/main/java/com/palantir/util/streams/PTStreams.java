package com.palantir.util.streams;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class PTStreams {
    private PTStreams() {
        //
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
    public static long copy(InputStream input, OutputStream output, byte[] buffer)
            throws IOException {
        long count = 0;
        int n = 0;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
            count += n;
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
