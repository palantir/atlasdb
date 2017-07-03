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

package com.palantir.atlasdb.http;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class JepsenLockClientTest {
    @SuppressWarnings("unchecked") // Mock in a test
    private final JepsenLockClient<String> mockClient = (JepsenLockClient<String>) mock(JepsenLockClient.class);
    private final StringLockClient client = new StringLockClient(mockClient);

    private static final String LOCK_TOKEN = "foo";

    @Test
    public void unlockSingleDoesNotCallUnlockWithNull() throws InterruptedException {
        client.unlockSingle(null);
        verify(mockClient, never()).unlock(any());
    }

    @Test
    public void unlockSingleCallsUnlockWithMatchingArgument() throws InterruptedException {
        client.unlockSingle(LOCK_TOKEN);
        verify(mockClient).unlock(eq(ImmutableSet.of(LOCK_TOKEN)));
    }

    @Test
    public void unlockSingleReturnsTrueIfTokenCanBeUnlocked() throws InterruptedException {
        when(mockClient.unlock(any())).thenReturn(ImmutableSet.of(LOCK_TOKEN));
        assertTrue(client.unlockSingle(LOCK_TOKEN));
    }

    @Test
    public void unlockSingleReturnsFalseIfTokenCannotBeUnlocked() throws InterruptedException {
        when(mockClient.unlock(any())).thenReturn(ImmutableSet.of());
        assertFalse(client.unlockSingle(LOCK_TOKEN));
    }

    @Test
    public void refreshSingleDoesNotCallRefreshWithNull() throws InterruptedException {
        client.refreshSingle(null);
        verify(mockClient, never()).refresh(any());
    }

    @Test
    public void refreshSingleCallsRefreshWithMatchingArgument() throws InterruptedException {
        client.refreshSingle(LOCK_TOKEN);
        verify(mockClient).refresh(eq(ImmutableSet.of(LOCK_TOKEN)));
    }

    @Test
    public void refreshSingleReturnsTokenIfCanBeRefreshed() throws InterruptedException {
        when(mockClient.refresh(any())).thenReturn(ImmutableSet.of(LOCK_TOKEN));
        assertThat(client.refreshSingle(LOCK_TOKEN), equalTo(LOCK_TOKEN));
    }

    @Test
    public void refreshSingleReturnsNullIfCannotRefresh() throws InterruptedException {
        when(mockClient.refresh(any())).thenReturn(ImmutableSet.of());
        assertThat(client.refreshSingle(LOCK_TOKEN), nullValue());
    }

    static class StringLockClient implements JepsenLockClient<String> {
        private final JepsenLockClient<String> delegate;

        StringLockClient(JepsenLockClient<String> delegate) {
            this.delegate = delegate;
        }

        @Override
        public String lock(String client, String lockName) throws InterruptedException {
            return delegate.lock(client, lockName);
        }

        @Override
        public Set<String> unlock(Set<String> strings) throws InterruptedException {
            return delegate.unlock(strings);
        }

        @Override
        public Set<String> refresh(Set<String> strings) throws InterruptedException {
            return delegate.refresh(strings);
        }
    }
}
