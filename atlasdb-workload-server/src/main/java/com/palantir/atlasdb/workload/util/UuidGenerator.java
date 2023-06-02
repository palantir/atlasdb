/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.util;

import com.palantir.atlasdb.buggify.impl.DefaultNativeSamplingSecureRandomFactory;
import java.security.SecureRandom;
import java.util.UUID;

/**
 * This class is used to generate UUIDs for the workload server. We do not want to use {@link UUID#randomUUID()}
 * because that is implemented with an internal instance of {@link SecureRandom} that is not guaranteed to natively
 * draw numbers from the external environment on each invocation.
 * <p>
 * {@link UUID#randomUUID()} also has some precautions to ensure that the generated UUID is a type 4 UUID. However,
 * as we do not expect to propagate these identifiers outside the context of the workload server and only intend to use
 * them for comparison (as opposed to methods like {@link UUID#timestamp()}, it suffices to simply generate bytes and
 * use them.
 */
public final class UuidGenerator {
    private static final SecureRandom SECURE_RANDOM = DefaultNativeSamplingSecureRandomFactory.INSTANCE.create();

    private UuidGenerator() {
        // Utility class
    }

    public static UUID randomUUID() {
        return new UUID(SECURE_RANDOM.nextLong(), SECURE_RANDOM.nextLong());
    }
}
