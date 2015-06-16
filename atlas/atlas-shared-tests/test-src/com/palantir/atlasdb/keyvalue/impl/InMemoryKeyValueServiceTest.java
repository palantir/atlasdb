// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.keyvalue.impl;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;

//BaseTest is not a valid superclass for fast tests, because it starts an embedded postgres process
//which makes it slow, but also makes it hard to clean up after in the context of FastTests
public class InMemoryKeyValueServiceTest extends AbstractAtlasDbKeyValueServiceTest {

    @Override
    protected KeyValueService getKeyValueService() {
        return new InMemoryKeyValueService(true);
    }
}
