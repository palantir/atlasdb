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

package com.palantir.atlasdb.timelock.auth.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.immutables.value.Value;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
public abstract class BCryptedSecret {
    private static final Logger log = LoggerFactory.getLogger(BCryptedSecret.class);

    abstract String hashedSecret();

    public final boolean check(Password password) {
        try {
            return BCrypt.checkpw(password.value(), hashedSecret());
        } catch (IllegalArgumentException e) {
            log.warn("BCrypt hash configured is invalid, so denying access", e);
            return false;
        }
    }

    @JsonCreator
    public static BCryptedSecret of(String hashedSecret) {
        return ImmutableBCryptedSecret.builder()
                .hashedSecret(hashedSecret)
                .build();
    }
}
