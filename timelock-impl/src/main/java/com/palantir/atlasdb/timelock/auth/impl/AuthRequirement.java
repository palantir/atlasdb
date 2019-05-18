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

package com.palantir.atlasdb.timelock.auth.impl;

public enum AuthRequirement {
    /**
     * All namespaces are locked, only privileged clients have access.
     */
    ALWAYS_REQUIRE,

    /**
     * None of the namespaces requires auth. All clients have access to all namespaces.
     */
    NEVER_REQUIRE,

    /**
     * A namespace is locked only if a client claimed exclusive privileges for that namespace. Only privileged
     * clients have access to locked namespaces.
     */
    PRIVILEGE_BASED
}
