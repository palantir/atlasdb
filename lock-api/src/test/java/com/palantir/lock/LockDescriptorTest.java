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
package com.palantir.lock;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;
import org.junit.Test;

public class LockDescriptorTest {

    private static final String HELLO_WORLD_LOCK_ID = "Hello world!";
    private static final String OPPENHEIMER_LOCK_ID = "Now, I am become Death, the destroyer of worlds.";
    private static final String MEANING_OF_LIFE_LOCK_ID = "~[42]";

    @Test
    public void testSimpleStringDescriptor() {
        testAsciiLockDescriptors("abc123");
        testAsciiLockDescriptors(HELLO_WORLD_LOCK_ID);
        testAsciiLockDescriptors(OPPENHEIMER_LOCK_ID);
        testAsciiLockDescriptors(MEANING_OF_LIFE_LOCK_ID);
        testAsciiLockDescriptors(HELLO_WORLD_LOCK_ID + "/" + OPPENHEIMER_LOCK_ID);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidSimpleStringDescriptor() {
        testAsciiLockDescriptors("");
    }

    @Test(expected = NullPointerException.class)
    public void testNullSimpleStringDescriptor() {
        testAsciiLockDescriptors(null);
    }

    @Test
    public void testEncodedStringDescriptor() {
        testEncodedLockDescriptors("a\tb\nc\rd");
        testEncodedLockDescriptors(HELLO_WORLD_LOCK_ID + "\n");
        testEncodedLockDescriptors("\t" + OPPENHEIMER_LOCK_ID);
        testEncodedLockId(new byte[0]);
        testEncodedLockId(new byte[] {0x00});
        testEncodedLockId(new byte[] {'h', 0x00, 0x10, 'i'});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidEncodedStringDescriptor() {
        testEncodedLockDescriptors("");
    }

    @Test(expected = NullPointerException.class)
    public void testNullEncodedStringDescriptor() {
        testEncodedLockDescriptors(null);
    }

    private void testAsciiLockDescriptors(String lockId) {
        assertThat(StringLockDescriptor.of(lockId).toString()).isEqualTo(expectedLockDescriptorToString(lockId));

        assertThat(ByteArrayLockDescriptor.of(stringToBytes(lockId)).toString())
                .isEqualTo(expectedLockDescriptorToString(lockId));
    }

    private void testEncodedLockDescriptors(String lockId) {
        assertThat(StringLockDescriptor.of(lockId).toString()).isEqualTo(expectedEncodedLockDescriptorToString(lockId));

        testEncodedLockId(stringToBytes(lockId));
    }

    private void testEncodedLockId(byte[] bytes) {
        assertThat(ByteArrayLockDescriptor.of(bytes).toString())
                .isEqualTo(expectedEncodedLockDescriptorToString(bytes));
    }

    private static String expectedLockDescriptorToString(String lockId) {
        assertThat(lockId).isNotNull();
        return "LockDescriptor [" + lockId + "]";
    }

    private static String expectedEncodedLockDescriptorToString(String lockId) {
        return expectedEncodedLockDescriptorToString(stringToBytes(lockId));
    }

    private static String expectedEncodedLockDescriptorToString(byte[] lockId) {
        assertThat(lockId).isNotNull();
        return "LockDescriptor [" + BaseEncoding.base16().encode(lockId) + "]";
    }

    @SuppressWarnings("checkstyle:jdkStandardCharsets") // StandardCharsets only in JDK 1.7+
    private static byte[] stringToBytes(String lockId) {
        return lockId.getBytes(Charsets.UTF_8);
    }
}
