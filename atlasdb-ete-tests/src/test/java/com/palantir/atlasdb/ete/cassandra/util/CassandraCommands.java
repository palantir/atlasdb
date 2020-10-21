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
package com.palantir.atlasdb.ete.cassandra.util;

import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.ete.EteSetup;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class CassandraCommands {
    private CassandraCommands() {
        // Utility class
    }

    public static void nodetoolFlush(String containerName) throws IOException, InterruptedException {
        EteSetup.execCliCommand(containerName, "nodetool flush");
    }

    public static void nodetoolCompact(String containerName) throws IOException, InterruptedException {
        EteSetup.execCliCommand(containerName, "nodetool compact");
    }

    public static List<String> nodetoolGetSsTables(
            String containerName, String keyspace, TableReference tableRef, byte[] rowKey)
            throws IOException, InterruptedException {
        String query = String.format(
                "nodetool getsstables %s %s %s",
                keyspace,
                CassandraKeyValueServiceImpl.internalTableName(tableRef),
                BaseEncoding.base16().lowerCase().encode(rowKey));
        String output = EteSetup.execCliCommand(containerName, query);

        String[] outputSplitIntoLines = output.split("\n");
        return Arrays.stream(outputSplitIntoLines)
                .skip(1) // first line is Picked up _JAVA_OPTIONS...
                .collect(Collectors.toList());
    }

    public static String ssTableMetadata(String containerName, String path) throws IOException, InterruptedException {
        return EteSetup.execCliCommand(containerName, "sstablemetadata " + path);
    }
}
