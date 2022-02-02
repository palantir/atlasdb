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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.api.DisableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.SingleNodeUpdateResponse;
import com.palantir.paxos.SqliteConnections;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DisabledNamespacesTest {
    private static final UUID LOCK_ID = new UUID(13, 52);
    private static final UUID OTHER_LOCK_ID = new UUID(123, 45);

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final Namespace FIRST = Namespace.of("fst");
    private static final Namespace SECOND = Namespace.of("snd");

    private DisabledNamespaces disabledNamespaces;

    @Before
    public void setup() {
        DataSource dataSource = SqliteConnections.getDefaultConfiguredPooledDataSource(
                tempFolder.getRoot().toPath());
        disabledNamespaces = DisabledNamespaces.create(dataSource);
    }

    @Test
    public void notDisabledByDefault() {
        assertThat(disabledNamespaces.isDisabled(FIRST)).isFalse();
        assertThat(disabledNamespaces.isDisabled(SECOND)).isFalse();
        assertThat(disabledNamespaces.disabledNamespaces()).isEmpty();
    }

    @Test
    public void canDisableSingleNamespace() {
        disabledNamespaces.disable(disableNamespacesRequest(FIRST));

        assertThat(disabledNamespaces.isDisabled(FIRST)).isTrue();
        assertThat(disabledNamespaces.isDisabled(SECOND)).isFalse();
        assertThat(disabledNamespaces.disabledNamespaces()).containsExactly(FIRST);
    }

    @Test
    public void canDisableMultipleNamespaces() {
        disabledNamespaces.disable(disableNamespacesRequest(FIRST));
        disabledNamespaces.disable(disableNamespacesRequest(SECOND));

        assertThat(disabledNamespaces.isDisabled(FIRST)).isTrue();
        assertThat(disabledNamespaces.isDisabled(SECOND)).isTrue();
        assertThat(disabledNamespaces.disabledNamespaces()).containsExactlyInAnyOrder(FIRST, SECOND);
    }

    @Test
    public void disableFailsIfAlreadyDisabled() {
        SingleNodeUpdateResponse firstResponse = disabledNamespaces.disable(disableNamespacesRequest(FIRST));
        assertThat(firstResponse).isEqualTo(successfulResponse());

        SingleNodeUpdateResponse secondResponse = disabledNamespaces.disable(disableNamespacesRequest(FIRST));
        assertThat(secondResponse).isEqualTo(unsuccessfulResponse());
    }

    @Test
    public void disableFailsIfPartiallyDisabled() {
        SingleNodeUpdateResponse firstResponse = disabledNamespaces.disable(disableNamespacesRequest(FIRST));
        assertThat(firstResponse).isEqualTo(successfulResponse());

        SingleNodeUpdateResponse secondResponse = disabledNamespaces.disable(disableNamespacesRequest(SECOND, FIRST));

        assertThat(disabledNamespaces.isDisabled(SECOND)).isFalse();
        assertThat(secondResponse).isEqualTo(unsuccessfulResponse());

        SingleNodeUpdateResponse thirdResponse = disabledNamespaces.disable(disableNamespacesRequest(FIRST, SECOND));

        assertThat(disabledNamespaces.isDisabled(SECOND)).isFalse();
        assertThat(thirdResponse).isEqualTo(unsuccessfulResponse());
    }

    @Test
    public void enableFailsIfDisabledWithWrongLock() {
        disabledNamespaces.disable(disableNamespacesRequest(FIRST));
        DisableNamespacesRequest wrongLockId = DisableNamespacesRequest.of(ImmutableSet.of(SECOND), OTHER_LOCK_ID);
        disabledNamespaces.disable(wrongLockId);

        SingleNodeUpdateResponse response =
                disabledNamespaces.reEnable(ReenableNamespacesRequest.of(ImmutableSet.of(FIRST, SECOND), LOCK_ID));

        assertThat(response).isEqualTo(SingleNodeUpdateResponse.of(false, ImmutableMap.of(SECOND, OTHER_LOCK_ID)));
    }

    @Test
    public void canReEnableNamespaces() {
        disabledNamespaces.disable(disableNamespacesRequest(FIRST));
        disabledNamespaces.disable(disableNamespacesRequest(SECOND));
        disabledNamespaces.reEnable(reEnableNamespacesRequest(FIRST));

        assertThat(disabledNamespaces.isDisabled(FIRST)).isFalse();
        assertThat(disabledNamespaces.isDisabled(SECOND)).isTrue();
        assertThat(disabledNamespaces.disabledNamespaces()).containsExactly(SECOND);

        disabledNamespaces.reEnable(reEnableNamespacesRequest(SECOND));
        assertThat(disabledNamespaces.isDisabled(SECOND)).isFalse();
        assertThat(disabledNamespaces.disabledNamespaces()).isEmpty();
    }

    @Test
    public void disablingAndReEnablingAreIdempotent() {
        disabledNamespaces.disable(disableNamespacesRequest(FIRST));
        disabledNamespaces.disable(disableNamespacesRequest(FIRST));
        disabledNamespaces.disable(disableNamespacesRequest(FIRST));

        assertThat(disabledNamespaces.isDisabled(FIRST)).isTrue();

        disabledNamespaces.reEnable(reEnableNamespacesRequest(FIRST));
        disabledNamespaces.reEnable(reEnableNamespacesRequest(FIRST));
        assertThat(disabledNamespaces.isDisabled(FIRST)).isFalse();
    }

    private DisableNamespacesRequest disableNamespacesRequest(Namespace... namespaces) {
        return DisableNamespacesRequest.of(ImmutableSet.copyOf(namespaces), LOCK_ID);
    }

    private ReenableNamespacesRequest reEnableNamespacesRequest(Namespace... namespaces) {
        return ReenableNamespacesRequest.of(ImmutableSet.copyOf(namespaces), LOCK_ID);
    }

    private SingleNodeUpdateResponse successfulResponse() {
        return SingleNodeUpdateResponse.successful();
    }

    private SingleNodeUpdateResponse unsuccessfulResponse() {
        return SingleNodeUpdateResponse.of(false, ImmutableMap.of(FIRST, LOCK_ID));
    }
}
