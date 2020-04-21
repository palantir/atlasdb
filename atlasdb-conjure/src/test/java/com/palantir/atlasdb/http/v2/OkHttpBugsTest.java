/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.http.v2;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.net.SocketTimeoutException;
import org.junit.Test;
import com.google.common.util.concurrent.UncheckedExecutionException;

public class OkHttpBugsTest {

    @Test
    public void testIsPossiblyOkHttpTimeoutBug() {
        assertThat(OkHttpBugs.isPossiblyOkHttpTimeoutBug(
                new RuntimeException(new UncheckedExecutionException(new SocketTimeoutException())))).isTrue();
        assertThat(OkHttpBugs.isPossiblyOkHttpTimeoutBug(new RuntimeException(new RuntimeException())))
                .isFalse();
    }
}
