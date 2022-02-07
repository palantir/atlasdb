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

package com.palantir.atlasdb.backup;

import static com.palantir.logsafe.testing.Assertions.assertThat;

import com.palantir.tokens.auth.AuthHeader;
import com.palantir.tokens.auth.BearerToken;
import java.util.Optional;
import org.junit.Test;

public class AuthHeaderValidatorTest {
    private static final BearerToken BEARER_TOKEN = BearerToken.valueOf("bear");
    private static final BearerToken WRONG_TOKEN = BearerToken.valueOf("tiger");

    private final AuthHeaderValidator authHeaderValidator = new AuthHeaderValidator(() -> Optional.of(BEARER_TOKEN));

    @Test
    public void succeedsWithMatchingHeader() {
        assertThat(authHeaderValidator.suppliedTokenIsValid(AuthHeader.of(BEARER_TOKEN)))
                .isTrue();
    }

    @Test
    public void failsWithWrongHeader() {
        assertThat(authHeaderValidator.suppliedTokenIsValid(AuthHeader.of(WRONG_TOKEN)))
                .isFalse();
    }

    @Test
    public void failsIfSupplierYieldsEmpty() {
        AuthHeaderValidator emptyValidator = new AuthHeaderValidator(Optional::empty);
        assertThat(emptyValidator.suppliedTokenIsValid(AuthHeader.of(BEARER_TOKEN)))
                .isFalse();
    }
}
