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
package com.palantir.atlasdb.schema;

import org.slf4j.Logger;

public class KvsMigrationMessageProcessorLoggingImpl implements KeyValueServiceMigrator.KvsMigrationMessageProcessor {
    private final Logger logger;

    public KvsMigrationMessageProcessorLoggingImpl(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void processMessage(String message, KeyValueServiceMigrator.KvsMigrationMessageLevel level) {
        switch (level) {
            case INFO:
                logger.info(message);
                break;
            case WARN:
                logger.warn(message);
                break;
            case ERROR:
                logger.error(message);
                break;
        }
    }
}
