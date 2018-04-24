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

import org.junit.Before;
import org.junit.Test;

public class SimpleCleanerTest {
    private Scrubber mockScrubber = mock(Scrubber.class);
    private Puncher mockPuncher = mock(Puncher.class);

    private SimpleCleaner simpleCleaner;

    @Before
    public void setUp() {
        simpleCleaner = new SimpleCleaner(mockScrubber, mockPuncher, () -> 1L);

        when(mockScrubber.isInitialized()).thenReturn(true);
        when(mockPuncher.isInitialized()).thenReturn(true);
    }

    @Test
    public void isInitializedWhenPrerequisitesAreInitialized() {
        assertTrue(simpleCleaner.isInitialized());
    }

    @Test
    public void isNotInitializedWhenScrubberIsNotInitialized() {
        when(mockScrubber.isInitialized()).thenReturn(false);
        assertFalse(simpleCleaner.isInitialized());
    }

    @Test
    public void isInitializedWhenPuncherIsNotInitialized() {
        when(mockPuncher.isInitialized()).thenReturn(false);
        assertFalse(simpleCleaner.isInitialized());
    }

}
