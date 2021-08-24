/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.nexus.db.sql.monitoring.logger;

import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("SLF4J_LOGGER_SHOULD_BE_PRIVATE")
public class SqlLoggers {
    public static final SafeLogger LOGGER = SafeLoggerFactory.get("com.palantir.nexus.db.SQL");
    public static final SafeLogger CANCEL_LOGGER = SafeLoggerFactory.get("com.palantir.nexus.db.SQL.cancel");
    public static final SafeLogger SQL_EXCEPTION_LOG =
            SafeLoggerFactory.get("sqlException.com.palantir.nexus.db.sql.BasicSQL");
}
