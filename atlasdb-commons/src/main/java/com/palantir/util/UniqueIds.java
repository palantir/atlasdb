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

package com.palantir.util;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.logsafe.Preconditions;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public final class UniqueIds {
    private UniqueIds() {}

    public static UUID uuid() {
        return uuid(ThreadLocalRandom.current());
    }

    @VisibleForTesting
    static UUID uuid(Random random) {
        return uuid(bytes(random));
    }

    private static UUID uuid(byte[] data) {
        Preconditions.checkArgument(data.length == 16, "Invalid data length, expected 16 bytes");
        data[6] = (byte) ((data[6] & 0x0f) | 0x40); // version 4
        data[8] = (byte) ((data[8] & 0x3f) | 0x80); // IETF variant

        long mostSigBits = 0;
        for (int i = 0; i < 8; i++) {
            mostSigBits = (mostSigBits << 8) | (data[i] & 0xff);
        }

        long leastSigBits = 0;
        for (int i = 8; i < 16; i++) {
            leastSigBits = (leastSigBits << 8) | (data[i] & 0xff);
        }

        return new UUID(mostSigBits, leastSigBits);
    }

    private static byte[] bytes(Random random) {
        byte[] data = new byte[16];
        random.nextBytes(data);
        return data;
    }
}
