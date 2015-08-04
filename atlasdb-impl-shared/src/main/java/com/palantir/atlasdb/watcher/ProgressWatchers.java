/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.watcher;

import org.slf4j.Logger;

import com.palantir.atlasdb.watcher.ProgressWatcher.Reporter;
import com.palantir.atlasdb.watcher.ProgressWatcher.Watch;

public class ProgressWatchers {
    public static ProgressWatcher createSimpleProgressWatcher() {
        return new SimpleProgressWatcher();
    }

    public static Watch createNullWatch() {
        return new SimpleProgressWatcher().watch("");
    }

    public static Reporter createLoggerReporter(Logger logger) {
        return new LoggerReporter(logger);
    }
}
