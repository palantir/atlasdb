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

/**
 * A reporter using an slf4j logger for output
 *
 * @author jweel
 */
final class LoggerReporter extends AbstractTextualReporter implements Reporter {
    private final Logger logger;

    public LoggerReporter(Logger logger) {
        this.logger = logger;
    }

    @Override
    protected void output(String msg) {
        logger.info(msg);
    }
}
