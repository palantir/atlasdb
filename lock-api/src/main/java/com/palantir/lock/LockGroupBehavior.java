/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.lock;

/**
 * Defines the behavior that the lock server takes when some of the requested locks
 * cannot be acquired.
 *
 * @author jtamer
 */
public enum LockGroupBehavior {

    /**
     * Instructs the lock server to acquire as many of the requested locks as
     * possible.
     */
    LOCK_AS_MANY_AS_POSSIBLE,

    /**
     * Instructs the lock server to not acquire any of the locks unless they can
     * all be acquired. This is the default behavior.
     */
    LOCK_ALL_OR_NONE;
}
