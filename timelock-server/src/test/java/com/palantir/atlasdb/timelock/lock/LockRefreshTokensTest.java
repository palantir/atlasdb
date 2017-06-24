/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

import org.junit.Test;

import com.palantir.atlasdb.timelock.LockRefreshTokens;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.v2.LockTokenV2;

public class LockRefreshTokensTest {

    @Test
    public void canConvertToAndFromLockTokenV2() {
        for (int i = 0; i < 100; i++) {
            UUID uuid = UUID.randomUUID();
            LockTokenV2 tokenV2 = LockTokenV2.of(uuid);

            LockRefreshToken converted = LockRefreshTokens.fromLockTokenV2(tokenV2);
            LockTokenV2 convertedBack = LockRefreshTokens.toLockTokenV2(converted);

            assertThat(convertedBack).isEqualTo(tokenV2);
        }
    }

}
