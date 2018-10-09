/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.jdbc;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.ClassRule;
import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueServiceTest;
import com.palantir.atlasdb.keyvalue.impl.CloseableResourceManager;

public class JdbcKeyValueSharedTest extends AbstractKeyValueServiceTest {
    @ClassRule
    public static final CloseableResourceManager KVS = new CloseableResourceManager(JdbcTests::createEmptyKvs);

    @Override
    protected KeyValueService getKeyValueService() {
        return KVS.getKvs();
    }

    @Override
    protected boolean reverseRangesSupported() {
        return true;
    }

    @Override
    protected boolean checkAndSetSupported() {
        return false;
    }

    @Override
    @Test
    public void clusterAvailabilityStatusShouldBeAllAvailable() {
        assertThatThrownBy(() -> getKeyValueService().getClusterAvailabilityStatus()).isInstanceOf(
                UnsupportedOperationException.class);
    }
}
