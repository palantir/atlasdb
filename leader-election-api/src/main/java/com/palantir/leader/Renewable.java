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

package com.palantir.leader;

/**
 * A renewable object is one that may undergo renewal. Typically, this is used for dynamic proxies: renewal means
 * that the underlying object should be discarded and recreated, even though other state around the proxy's
 * life-cycle need not be updated. For example, in the context of a dynamic proxy guarded by leader election, it may be
 * the case that we want to reset all proxies even though a leader election has not occurred. In that case, renewing
 * all relevant proxies is the correct action.
 */
public interface Renewable {
    void renew();
}
