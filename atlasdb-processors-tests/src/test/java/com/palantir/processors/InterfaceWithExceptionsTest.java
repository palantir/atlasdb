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

package com.palantir.processors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;

public class InterfaceWithExceptionsTest {
    final InterfaceWithExceptions delegateMock = mock(InterfaceWithExceptions.class);
    boolean handleIllegalStateExceptionWasCalled = false;
    boolean handleRuntimeExceptionWasCalled = false;

    final AutoDelegate_InterfaceWithExceptions wrapper = new AutoDelegate_InterfaceWithExceptions() {
        @Override
        public InterfaceWithExceptions delegate() {
            return delegateMock;
        }

        @Override
        public void handleIllegalStateException(IllegalStateException exception) {
            handleIllegalStateExceptionWasCalled = true;
        }

        @Override
        public void handleRuntimeException(RuntimeException exception) {
            handleRuntimeExceptionWasCalled = true;
        }
    };

    @Before
    public void setUp() {
        handleIllegalStateExceptionWasCalled = false;
        handleRuntimeExceptionWasCalled = false;
    }

    @Test
    public void assertIllegalStateExceptionIsCaughtByHandler() {
        doThrow(IllegalStateException.class).when(delegateMock).methodThatThrowsExceptions();
        wrapper.methodThatThrowsExceptions();
        assertTrue(handleIllegalStateExceptionWasCalled);
        assertFalse(handleRuntimeExceptionWasCalled);
    }

    @Test
    public void assertRuntimeExceptionIsCaughtByHandler() {
        doThrow(RuntimeException.class).when(delegateMock).methodThatThrowsExceptions();
        wrapper.methodThatThrowsExceptions();
        assertFalse(handleIllegalStateExceptionWasCalled);
        assertTrue(handleRuntimeExceptionWasCalled);
    }
}
