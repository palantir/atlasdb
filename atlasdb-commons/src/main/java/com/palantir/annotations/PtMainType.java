/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.annotations;

/**
 *
 *
 */
public enum PtMainType {
    /**
     * For older runnable classes.
     */
    LEGACY,

    /**
     * Only for use with CLIs that are used in a production environment.
     */
    PRODUCTION_CLI,

    /**
     * Launches a server either through a daemon process or in a blocking
     * manner.
     */
    SERVER,

    /**
     * Launches a GUI that requires a headed client.
     */
    GUI,

    /**
     * Used to test code in simulation or dev environments.
     */
    TEST,

    /**
     * For development use only, not for use in production.
     */
    DEV, ;
}
