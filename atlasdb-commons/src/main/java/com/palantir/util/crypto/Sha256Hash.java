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
package com.palantir.util.crypto;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * A SHA-256 hash. This class provides type-safety and equals/hashCode
 * implementations.
 */
public class Sha256Hash implements Serializable, Comparable<Sha256Hash> {
    private static final long serialVersionUID = 1L;

    /** The hash of an empty byte array, which can be used as a sentinel value. */
    public static final Sha256Hash EMPTY = Sha256Hash.computeHash(new byte[0]);

    private final byte[] bytes;
    private transient int hashCode = 0;

    /**
     * Constructs a type-safe {@link Sha256Hash} object from the given hash
     * bytes. The given byte array must be exactly 256 bits (32 bytes) in
     * length.
     */
    public Sha256Hash(byte[] bytes) {
        if (bytes.length != 32) {
            throw new SafeIllegalArgumentException("Incorrect SHA-256 hash size."); // $NON-NLS-1$
        }
        this.bytes = bytes.clone();
    }

    public Sha256Hash(HashCode hashCode) {
        this.bytes = hashCode.asBytes();
        if (this.bytes.length != 32) {
            throw new SafeIllegalArgumentException("Incorrect SHA-256 hash size."); // $NON-NLS-1$
        }
    }

    public static Sha256Hash createFrom(MessageDigest digest) {
        return new Sha256Hash(digest.digest());
    }

    /**
     * This method will read all the bytes in the stream and digest them.
     * It will attempt to close the stream as well.
     */
    public static Sha256Hash createFrom(InputStream is) throws IOException {
        try {
            MessageDigest digest = getMessageDigest();
            DigestInputStream digestInputStream = new DigestInputStream(is, digest);
            ByteStreams.copy(digestInputStream, ByteStreams.nullOutputStream());
            digestInputStream.close();
            return createFrom(digest);
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                /* don't throw from finally*/
            }
        }
    }

    public Hasher hash(Hasher hasher) {
        return hasher.putBytes(bytes);
    }

    public String serializeToBase64String() {
        return BaseEncoding.base64().encode(bytes);
    }

    public static Sha256Hash deSerializeFromBase64String(String s) {
        return new Sha256Hash(BaseEncoding.base64().decode(s));
    }

    public String serializeToHexString() {
        return BaseEncoding.base16().lowerCase().encode(bytes);
    }

    public static Sha256Hash deSerializeFromHexString(String s) {
        return new Sha256Hash(BaseEncoding.base16().lowerCase().decode(s.toLowerCase()));
    }

    public byte[] getBytes() {
        return bytes.clone();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !obj.getClass().equals(getClass())) {
            return false;
        }
        // Use constant speed equals
        return MessageDigest.isEqual(bytes, ((Sha256Hash) obj).bytes);
    }

    @Override
    public int hashCode() {
        // same mechanism as java.lang.String. Note: Don't return hashCode directly
        int localHash = hashCode;
        if (localHash == 0) {
            localHash = Arrays.hashCode(bytes);
            hashCode = localHash;
        }
        return localHash;
    }

    @Override
    public String toString() {
        if (equals(Sha256Hash.EMPTY)) {
            return "EMPTY"; //$NON-NLS-1$
        }
        // Converts the hash into a hexadecimal string.
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < bytes.length; ++i) {
            s.append(String.format("%02x", bytes[i])); // $NON-NLS-1$
        }
        return s.toString();
    }

    /** Returns a {@link MessageDigest} for computing SHA-256 hashes. */
    public static MessageDigest getMessageDigest() {
        return MessageDigestPrototype.SHA_256.newDigest();
    }

    /** Computes the SHA-256 hash for the given array of bytes. */
    public static Sha256Hash computeHash(byte[] bytes) {
        MessageDigest digest = getMessageDigest();
        return new Sha256Hash(digest.digest(bytes));
    }

    @Override
    public int compareTo(Sha256Hash o) {
        return UnsignedBytes.lexicographicalComparator().compare(bytes, o.bytes);
    }

    /**
     * Use {@link MessageDigest} prototypes as a workaround for
     * https://bugs.openjdk.java.net/browse/JDK-7092821, similar to Guava's
     * workaround https://github.com/google/guava/issues/1197
     */
    private enum MessageDigestPrototype {
        SHA_256("SHA-256"); // $NON-NLS-1$

        private final String algorithm;
        private final MessageDigest prototype;
        private final boolean supportsClone;

        MessageDigestPrototype(String algorithm) {
            this.algorithm = Preconditions.checkNotNull(algorithm);
            this.prototype = createDigest(algorithm);
            this.supportsClone = supportsClone(prototype);
        }

        public MessageDigest newDigest() {
            if (supportsClone) {
                try {
                    return (MessageDigest) prototype.clone();
                } catch (CloneNotSupportedException e) {
                    // fall through
                }
            }
            return createDigest(algorithm);
        }

        private static boolean supportsClone(MessageDigest prototype) {
            try {
                prototype.clone();
                return true;
            } catch (CloneNotSupportedException e) {
                return false;
            }
        }

        private static MessageDigest createDigest(String algorithm) {
            try {
                return MessageDigest.getInstance(algorithm);
            } catch (NoSuchAlgorithmException e) {
                // This should never happen.
                throw new IllegalArgumentException("Invalid message digest: " + e.getMessage(), e);
            }
        }
    }
}
