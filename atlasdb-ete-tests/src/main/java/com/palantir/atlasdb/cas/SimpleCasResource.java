/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.cas;

import com.google.common.base.Optional;

public class SimpleCasResource implements CasResource {
    private CasClient casClient;

    public SimpleCasResource(CasClient casClient) {
        this.casClient = casClient;
    }

    @Override
    public void set(Optional<Long> value) {
        casClient.set(value);
    }

    @Override
    public Long get() {
        return casClient.get().orNull();
    }

    @Override
    public boolean cas(CasValuePatch values) {
        return casClient.cas(values.getOldValue(), values.getNewValue());
    }
}
