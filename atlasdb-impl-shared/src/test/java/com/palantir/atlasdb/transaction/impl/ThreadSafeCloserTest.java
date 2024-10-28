/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;

import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.io.Closeable;
import java.io.IOException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ThreadSafeCloserTest {
    ThreadSafeCloser closer = new ThreadSafeCloser();

    @Test
    void closes_onClose() throws IOException {
        Closeable closeable = Mockito.mock(Closeable.class);
        closer.register(closeable);
        closer.close();
        verify(closeable).close();
    }

    @Test
    @Disabled
    void throws_ifAlreadyClosed() throws IOException {
        closer.close();
        Closeable closeable = Mockito.mock(Closeable.class);
        assertThatThrownBy(() -> closer.register(closeable)).isInstanceOf(SafeIllegalStateException.class);
        verify(closeable).close();
    }
}
