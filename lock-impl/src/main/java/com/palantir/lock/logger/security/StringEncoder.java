/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.lock.logger.security;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import com.google.common.annotations.VisibleForTesting;

/**
 * Created by davidt on 4/12/17.
 */
public class StringEncoder {

    private final SecretKey sercretKey;
    private static final String ALGORITHM_NAME = "AES";
    private static final String CIPHER_NAME = "AES/ECB/PKCS5Padding";
    private static final int KEY_SIZE = 128;

    public StringEncoder() {
        this.sercretKey = keyGen();
    }

    public String encrypt(String value) {
        Cipher cipher;
        try {
            cipher = Cipher.getInstance(CIPHER_NAME);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
            throw new RuntimeException(e);
        }

        try {
            cipher.init(Cipher.ENCRYPT_MODE, this.sercretKey);
        } catch (InvalidKeyException e) {
            throw new IllegalArgumentException(e);
        }

        try {
            byte[] encoded = cipher.doFinal(value.getBytes(StandardCharsets.ISO_8859_1));
            String s = new String(encoded, StandardCharsets.ISO_8859_1);
            return s;
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    String decrypt(String value) {
        Cipher cipher;
        try {
            cipher = Cipher.getInstance(CIPHER_NAME);
            cipher.init(Cipher.DECRYPT_MODE, sercretKey);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
            throw new IllegalArgumentException(e);
        }

        byte[] decrypted;
        try {
            decrypted = cipher.doFinal(value.getBytes(StandardCharsets.ISO_8859_1));
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new IllegalArgumentException(e);
        }
        return new String(decrypted, StandardCharsets.ISO_8859_1);
    }


    private SecretKey keyGen() {
        KeyGenerator keygen;
        try {
            keygen = KeyGenerator.getInstance(ALGORITHM_NAME);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(e);
        }
        keygen.init(KEY_SIZE);
        return keygen.generateKey();
    }

}
