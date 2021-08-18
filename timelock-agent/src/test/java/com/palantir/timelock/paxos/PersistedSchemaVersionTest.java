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

package com.palantir.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.paxos.SqliteConnections;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PersistedSchemaVersionTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private PersistedSchemaVersion schemaVersion;

    @Before
    public void setup() {
        schemaVersion = PersistedSchemaVersion.create(SqliteConnections.getDefaultConfiguredPooledDataSource(
                tempFolder.getRoot().toPath()));
    }

    @Test
    public void throwIfNoPersistedVersion() {
        assertThatThrownBy(() -> schemaVersion.getVersion()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void canSetInitialVersion() {
        schemaVersion.upgradeVersion(3L);
        assertThat(schemaVersion.getVersion()).isEqualTo(3L);
    }

    @Test
    public void canUpgradeVersion() {
        schemaVersion.upgradeVersion(3L);
        schemaVersion.upgradeVersion(5L);
        assertThat(schemaVersion.getVersion()).isEqualTo(5L);
    }

    @Test
    public void cannotDowngradeVersion() {
        schemaVersion.upgradeVersion(3L);
        schemaVersion.upgradeVersion(2L);
        assertThat(schemaVersion.getVersion()).isEqualTo(3L);
    }

    @Test
    public void canSetToMaxLong() {
        schemaVersion.upgradeVersion(Long.MAX_VALUE);
        assertThat(schemaVersion.getVersion()).isEqualTo(Long.MAX_VALUE);
    }
}
