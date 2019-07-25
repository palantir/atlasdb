/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.migration;

import static org.mockito.Mockito.mock;

import org.junit.Test;

import com.palantir.atlasdb.coordination.CoordinationServiceImpl;

@SuppressWarnings("unchecked") // Mocks of generic types
public class MigrationCoordinationServiceImplTest {
    private final CoordinationServiceImpl<TableMigrationStateMap> coordinationService =
            mock(CoordinationServiceImpl.class);
    private final MigrationCoordinationStateTransformer migrationCoordinationStateTransformer = mock(
            MigrationCoordinationStateTransformer.class);

    private final MigrationCoordinationServiceImpl migrationCoordinationService =
            new MigrationCoordinationServiceImpl(coordinationService, migrationCoordinationStateTransformer);

    @Test
    public void test() {
        System.out.println("blah");
    }
}
