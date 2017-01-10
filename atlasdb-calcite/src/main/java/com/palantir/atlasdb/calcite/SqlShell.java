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
package com.palantir.atlasdb.calcite;

import java.io.IOException;

import javax.inject.Inject;

import com.google.common.collect.ImmutableList;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;
import sqlline.SqlLine;

@Command(name = "asql", description = "AtlasDB SQL Shell.")
public class SqlShell {
    @Inject
    private HelpOption helpOption;

    @Arguments(usage = "atlas-config-file", description = "Path to the AtlasDB configuration YAML file.")
    String atlasConfigFile;

    @Option(name = {"--debug"}, description = "Enables debug mode for more logging.")
    private boolean debug = false;

    @Option(name = {"--align-columns"},
            description = "Aligns table columns by overfetching on each sql query to determine maximum column width. "
                    + "This is not performant, but for small tables is a visual improvement.")
    private boolean alignColumns = false;

    public static void main(String[] args) throws Exception {
        SqlShell cli = SingleCommand.singleCommand(SqlShell.class).parse(args);
        if (cli.atlasConfigFile == null) {
            cli.helpOption.help = true;
        }
        if (cli.helpOption.showHelpIfRequested()) {
            return;
        }
        cli.run();
    }

    private void run() throws IOException {
        ImmutableList.Builder<String> args = ImmutableList.<String>builder()
                .add("-u")
                .add(String.format("jdbc:calcite:"
                                + "schemaFactory=%s;"
                                + "schema.configFile=%s;"
                                + "lex=MYSQL_ANSI;"
                                + "schema=atlas",
                        AtlasSchemaFactory.class.getName(),
                        atlasConfigFile))
                .add("-d")
                .add("org.apache.calcite.jdbc.Driver")
                .add("--color=true");
        if (debug) {
            args.add("--verbose").add("--showNestedErrs");
        }
        if (alignColumns) {
            args.add("--incremental=false");
        }
        ImmutableList<String> argsList = args.build();
        SqlLine.main(argsList.toArray(new String[argsList.size()]));
    }
}
