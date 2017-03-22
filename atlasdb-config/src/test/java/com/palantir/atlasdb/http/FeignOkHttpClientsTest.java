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
package com.palantir.atlasdb.http;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import com.palantir.lock.RemoteLockService;
import com.palantir.lock.client.LockRefreshingRemoteLockService;

public class FeignOkHttpClientsTest {
    @Test
    public void classesSpecifiedNotToRetryAreNotRetriable() {
        FeignOkHttpClients.CLASSES_TO_NOT_RETRY.forEach(
                clazz -> assertThat(FeignOkHttpClients.shouldAllowRetrying(clazz)).isFalse());
    }

    @Test
    public void remoteLockServiceIsNotRetriable() {
        assertThat(FeignOkHttpClients.shouldAllowRetrying(RemoteLockService.class)).isFalse();
    }

    @Test
    public void subclassesOfClassesSpecifiedNotToRetryAreRetriable() {
        assertThat(FeignOkHttpClients.shouldAllowRetrying(ExtendedRemoteLockService.class)).isTrue();
        assertThat(FeignOkHttpClients.shouldAllowRetrying(LockRefreshingRemoteLockService.class)).isTrue();
    }

    @Test
    public void classesNotSpecifiedNotToRetryAreRetriable() {
        assertThat(FeignOkHttpClients.shouldAllowRetrying(AtomicReference.class)).isTrue();
    }

    private interface ExtendedRemoteLockService extends RemoteLockService {
        // Marker interface used for testing that subclasses aren't affected
    }
}
