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
package com.palantir.atlasdb.ete.dropwizard;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import com.palantir.atlasdb.ete.EteSetup;

public abstract class DropwizardEteTest extends EteSetup {
    private static final Pattern TIMESTAMP_REGEX = Pattern.compile("The Fresh timestamp is: (\\d+)");

    @Test
    public void multipleTimestampFetchesAreDifferent() throws IOException, InterruptedException {
        int timestamp1 = fetchTimestamp();
        int timestamp2 = fetchTimestamp();

        assertThat(timestamp1).isLessThan(timestamp2);
    }

    @Test
    public void sweepAllTablesDoesntError() throws IOException, InterruptedException {
        runCommand("service/bin/atlasdb-ete atlasdb sweep -a var/conf/atlasdb-ete.yml");
    }

    @Test
    public void consoleShouldLoadAndConnectToDb() throws IOException, InterruptedException {
        String output = runCommand("echo | service/bin/atlasdb-ete atlasdb console var/conf/atlasdb-ete.yml");

        assertThat(output).contains("//AtlasConsole started!");
    }

    private int fetchTimestamp() throws IOException, InterruptedException {
        String timestampFetched = runCommand("service/bin/atlasdb-ete atlasdb timestamp fetch var/conf/atlasdb-ete.yml");

        Matcher matcher = TIMESTAMP_REGEX.matcher(timestampFetched);
        assertThat(matcher.find()).isTrue();

        return Integer.parseInt(matcher.group(1));
    }
}
