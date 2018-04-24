/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.containers;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CassandraCliParser {
    private static final Logger log = LoggerFactory.getLogger(CassandraCliParser.class);

    private final CassandraVersion cassandraVersion;

    public CassandraCliParser(CassandraVersion cassandraVersion) {
        this.cassandraVersion = cassandraVersion;
    }

    public int parseSystemAuthReplicationFromCqlsh(String output) throws IllegalArgumentException {
        try {
            for (String line : output.split("\n")) {
                if (line.contains("system_auth")) {
                    Pattern replicationRegex = cassandraVersion.replicationFactorRegex();
                    Matcher matcher = replicationRegex.matcher(line);
                    matcher.find();
                    return Integer.parseInt(matcher.group(1));
                }
            }
        } catch (Exception e) {
            log.error("Failed parsing system_auth keyspace RF", e);
            throw new IllegalArgumentException("Cannot determine replication factor of system_auth keyspace");
        }

        throw new IllegalArgumentException("Cannot determine replication factor of system_auth keyspace");
    }

    public int parseNumberOfUpNodesFromNodetoolStatus(String output) {
        Pattern pattern = Pattern.compile("^UN.*");
        int upNodes = 0;
        for (String line : output.split("\n")) {
            Matcher matcher = pattern.matcher(line);
            if (matcher.matches()) {
                upNodes++;
            }
        }
        return upNodes;
    }
}
