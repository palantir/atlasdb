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
package com.palantir.atlasdb.cli.api;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.palantir.atlasdb.cli.impl.AtlasDbServicesImpl;

public abstract class AbstractAtlasDbCli {

    @Option(name = "--help", aliases = { "-h" }, usage = "this help information", help = true)
    boolean help = false;

    @Option(name = "--config", aliases = { "-c" }, required = true, usage = "path to yaml configuration file for atlasdb")
    String configFileName;

    public int run(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
            if (help) {
                printUsage(parser);
                return 0;
            } else {
                return execute(AtlasDbServicesImpl.connect(this.configFileName));
            }
        } catch (CmdLineException e) {
            e.printStackTrace();
            printUsage(parser);
            return 1;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    private void printUsage(CmdLineParser parser) {
        System.out.println(this.getClass().getSimpleName() + " Usage:");
        parser.printUsage(System.out);
    }

    public abstract int execute(AtlasDbServices services);

}
