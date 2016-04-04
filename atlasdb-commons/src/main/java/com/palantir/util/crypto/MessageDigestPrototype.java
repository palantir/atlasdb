/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.util.crypto;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.google.common.base.Preconditions;

/**
 * Use {@link MessageDigest} prototypes as a workaround for
 * https://bugs.openjdk.java.net/browse/JDK-7092821, similar to Guava's
 * workaround https://github.com/google/guava/issues/1197
 */
public enum MessageDigestPrototype {
    MD5("MD5"), //$NON-NLS-1$
    SHA1("SHA1"), //$NON-NLS-1$
    SHA_256("SHA-256"); //$NON-NLS-1$

    private final String algorithm;
    private final MessageDigest prototype;
    private final boolean supportsClone;

    private MessageDigestPrototype(String algorithm) {
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
