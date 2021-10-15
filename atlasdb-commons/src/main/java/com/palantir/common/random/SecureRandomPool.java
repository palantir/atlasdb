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
package com.palantir.common.random;

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class SecureRandomPool {
    private static final SafeLogger log = SafeLoggerFactory.get(SecureRandomPool.class);

    private final List<SecureRandom> pool;
    private final SecureRandom seedSource;
    private final AtomicLong next = new AtomicLong(0L);

    /**
     * Creates a SecureRandomPool using the specified algorithm.
     */
    public SecureRandomPool(String algorithm, int poolSize) {
        this(algorithm, poolSize, null);
    }

    /**
     * Creates a SecureRandomPool using the specified algorithm.  The provided
     * SecureRandom is used to seed each new SecureRandom in the pool.
     */
    public SecureRandomPool(String algorithm, int poolSize, SecureRandom seed) {
        if (algorithm == null) {
            throw new SafeIllegalArgumentException("algorithm is null");
        }

        pool = new ArrayList<SecureRandom>(poolSize);
        seedSource = getSeedSource(algorithm, (seed != null) ? seed : new SecureRandom());

        try {
            for (int i = 0; i < poolSize; i++) {
                byte[] seedBytes = new byte[20];
                seedSource.nextBytes(seedBytes);
                SecureRandom random = SecureRandom.getInstance(algorithm); // (authorized)
                random.setSeed(seedBytes);
                pool.add(random);
            }
        } catch (NoSuchAlgorithmException e) {
            log.error("Error getting SecureRandom using {} algorithm.", SafeArg.of("algorithm", algorithm), e);
            throw new RuntimeException(String.format("Error getting SecureRandom using %s algorithm.", algorithm), e);
        }
    }

    public SecureRandom getSecureRandom() {
        int i = (int) Math.abs(next.getAndIncrement() % pool.size());
        return pool.get(i);
    }

    /**
     * 1. The provided seed is the initial seed.  On Linux, it likely reads from
     * /dev/random or /dev/urandom and is potentially very slow.<br/>
     * 2. The initial seed is then used to create a source seed based on the provided
     * algorithm.  This algorithm should be something fast, such as "SHA1PRNG".<br/>
     * 3. The source seed is then used to seed all of the SecureRandoms in the
     * pool.  If the algorithm is fast, then initialization of the pool should
     * fast as well.<br/>
     */
    private SecureRandom getSeedSource(String algorithm, SecureRandom seed) {
        try {
            SecureRandom seedSource = SecureRandom.getInstance(algorithm); // (authorized)
            byte[] seedBytes = new byte[20];
            seed.nextBytes(seedBytes);
            seedSource.setSeed(seedBytes);
            return seedSource;
        } catch (NoSuchAlgorithmException e) {
            log.error(
                    "Error getting SecureRandom using {} algorithm for seed source.",
                    SafeArg.of("algorithm", algorithm),
                    e);
            return seed;
        }
    }
}
