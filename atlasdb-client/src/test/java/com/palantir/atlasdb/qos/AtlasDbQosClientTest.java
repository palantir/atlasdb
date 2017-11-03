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

package com.palantir.atlasdb.qos;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.naming.LimitExceededException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class AtlasDbQosClientTest {
    private QosService qosService = mock(QosService.class);

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        when(qosService.getLimit("test-client")).thenReturn(1);
    }

    @Test
    public void doesNotBackOff() throws LimitExceededException {
        AtlasDbQosClient qosClient = AtlasDbQosClient.create(qosService, "test-client");
        qosClient.checkLimit();
    }

    @Test
    public void throwsAfterLimitExceeded() throws LimitExceededException {
        AtlasDbQosClient qosClient = AtlasDbQosClient.create(qosService, "test-client");
        qosClient.checkLimit();

        exception.expect(LimitExceededException.class);
        qosClient.checkLimit();
    }

}
