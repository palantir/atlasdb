/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.lock;

import com.palantir.common.proxy.SerializingProxy;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.lock.logger.LockServiceTestUtils;

public final class LockServiceIntegrationTest extends LockServiceTest {
    @Override
    protected LockService getLockService() {
        return SerializingProxy.newProxyInstance(LockService.class, LockServiceImpl.create(
                LockServerOptions.builder()
                .isStandaloneServer(false)
                .lockStateLoggerDir(LockServiceTestUtils.TEST_LOG_STATE_DIR)
                .build()));
    }
}
