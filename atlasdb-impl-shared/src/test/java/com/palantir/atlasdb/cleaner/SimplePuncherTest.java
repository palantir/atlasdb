/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.cleaner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.palantir.common.time.Clock;

public class SimplePuncherTest {
    @Test
    public void delegatesInitializationCheck() {
        PuncherStore delegate = mock(PuncherStore.class);
        Puncher puncher = SimplePuncher.create(delegate, mock(Clock.class), () -> 1L);

        when(delegate.isInitialized())
                .thenReturn(false)
                .thenReturn(true);

        assertFalse(puncher.isInitialized());
        assertTrue(puncher.isInitialized());
    }
}
