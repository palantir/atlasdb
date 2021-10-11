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
package com.palantir.atlasdb.containers;

import com.google.common.base.Splitter;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class CassandraCliParser {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraCliParser.class);

    private final CassandraVersion cassandraVersion;

    public CassandraCliParser(CassandraVersion cassandraVersion) {
        this.cassandraVersion = cassandraVersion;
    }

    public int parseSystemAuthReplicationFromCqlsh(String output) throws IllegalArgumentException {
        try {
            for (String line : Splitter.on('\n').split(output)) {
                if (line.contains("system_auth")) {
                    Pattern replicationRegex = cassandraVersion.replicationFactorRegex();
                    Matcher matcher = replicationRegex.matcher(line);
                    matcher.find();
                    return Integer.parseInt(matcher.group(1));
                }
            }
        } catch (Exception e) {
            log.error("Failed parsing system_auth keyspace RF", e);
            throw new SafeIllegalArgumentException("Cannot determine replication factor of system_auth keyspace");
        }

        throw new SafeIllegalArgumentException("Cannot determine replication factor of system_auth keyspace");
    }

    public int parseNumberOfUpNodesFromNodetoolStatus(String output) {
        Pattern pattern = Pattern.compile("^UN.*");
        int upNodes = 0;
        for (String line : Splitter.on('\n').split(output)) {
            Matcher matcher = pattern.matcher(line);
            if (matcher.matches()) {
                upNodes++;
            }
        }
        return upNodes;
    }
}
