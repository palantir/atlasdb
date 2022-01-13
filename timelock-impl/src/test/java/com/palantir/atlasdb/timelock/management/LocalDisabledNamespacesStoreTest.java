/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.management;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.paxos.SqliteConnections;
import javax.sql.DataSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LocalDisabledNamespacesStoreTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final String FIRST = "fst";
    private static final String SECOND = "snd";

    private LocalDisabledNamespacesStore localDisabledNamespacesStore;

    @Before
    public void setup() {
        DataSource dataSource = SqliteConnections.getDefaultConfiguredPooledDataSource(
                tempFolder.getRoot().toPath());
        localDisabledNamespacesStore = LocalDisabledNamespacesStore.create(dataSource);
    }

    @Test
    public void notDisabledByDefault() {
        assertThat(localDisabledNamespacesStore.isDisabled(FIRST)).isFalse();
        assertThat(localDisabledNamespacesStore.isDisabled(SECOND)).isFalse();
        assertThat(localDisabledNamespacesStore.disabledNamespaces()).isEmpty();
    }

    @Test
    public void canDisableSingleNamespace() {
        localDisabledNamespacesStore.disable(FIRST);

        assertThat(localDisabledNamespacesStore.isDisabled(FIRST)).isTrue();
        assertThat(localDisabledNamespacesStore.isDisabled(SECOND)).isFalse();
        assertThat(localDisabledNamespacesStore.disabledNamespaces()).containsExactly(FIRST);
    }

    @Test
    public void canDisableMultipleNamespaces() {
        localDisabledNamespacesStore.disable(FIRST);
        localDisabledNamespacesStore.disable(SECOND);

        assertThat(localDisabledNamespacesStore.isDisabled(FIRST)).isTrue();
        assertThat(localDisabledNamespacesStore.isDisabled(SECOND)).isTrue();
        assertThat(localDisabledNamespacesStore.disabledNamespaces()).containsExactlyInAnyOrder(FIRST, SECOND);
    }

    @Test
    public void canReEnableNamespaces() {
        localDisabledNamespacesStore.disable(FIRST);
        localDisabledNamespacesStore.disable(SECOND);
        localDisabledNamespacesStore.reEnable(FIRST);

        assertThat(localDisabledNamespacesStore.isDisabled(FIRST)).isFalse();
        assertThat(localDisabledNamespacesStore.isDisabled(SECOND)).isTrue();
        assertThat(localDisabledNamespacesStore.disabledNamespaces()).containsExactly(SECOND);

        localDisabledNamespacesStore.reEnable(SECOND);
        assertThat(localDisabledNamespacesStore.isDisabled(SECOND)).isFalse();
        assertThat(localDisabledNamespacesStore.disabledNamespaces()).isEmpty();
    }

    @Test
    public void disablingAndReEnablingAreIdempotent() {
        localDisabledNamespacesStore.disable(FIRST);
        localDisabledNamespacesStore.disable(FIRST);
        localDisabledNamespacesStore.disable(FIRST);

        assertThat(localDisabledNamespacesStore.isDisabled(FIRST)).isTrue();

        localDisabledNamespacesStore.reEnable(FIRST);
        localDisabledNamespacesStore.reEnable(FIRST);
        assertThat(localDisabledNamespacesStore.isDisabled(FIRST)).isFalse();
    }
}
