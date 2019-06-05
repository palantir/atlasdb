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

package com.palantir.atlasdb.autobatch;

import java.util.Map;
import java.util.Set;

public interface CoalescingRequestConsumer<REQUEST> extends CoalescingRequestFunction<REQUEST, Void> {
    @Override
    default Void defaultValue(){
        return null;
    }

    @Override
    default Map<REQUEST, Void> apply(Set<REQUEST> input) {
        accept(input);
        return null;
    }

    void accept(Set<REQUEST> elements);
}
