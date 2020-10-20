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
package com.palantir.atlasdb.console;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.console.module.AtlasCoreModule;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.tools.shell.Main;

public class AtlasConsoleMain {

    private static final String HELP_FLAG_SHORT = "h";
    private static final String HELP_FLAG_LONG = "help";
    private static final String CLASSPATH_FLAG_SHORT = "cp";
    private static final String CLASSPATH_FLAG_LONG = "classpath";
    private static final String SCRIPT_FLAG_SHORT = "s";
    private static final String SCRIPT_FLAG_LONG = "script";
    private static final String MUTATIONS_ENABLED_FLAG_SHORT = "m";
    private static final String MUTATIONS_ENABLED_FLAG_LONG = "mutations_enabled";
    private static final String EVAL_FLAG_SHORT = "e";
    private static final String EVAL_FLAG_LONG = "evaluate";
    private static final String BIND_FLAG_SHORT = "b";
    private static final String BIND_FLAG_LONG = "bind";

    public static final Options OPTIONS = new Options()
            .addOption(HELP_FLAG_SHORT, HELP_FLAG_LONG, false, "Prints help message.")
            .addOption(
                    SCRIPT_FLAG_SHORT,
                    SCRIPT_FLAG_LONG,
                    false,
                    "Path to .groovy file to execute as non-interactive application")
            .addOption(
                    MUTATIONS_ENABLED_FLAG_SHORT,
                    MUTATIONS_ENABLED_FLAG_LONG,
                    false,
                    "Enable put() and delete() commands for database mutation. "
                            + "THIS SHOULD ONLY BE ENABLED IF YOU REALLY KNOW WHAT YOU ARE DOING")
            .addOption(
                    EVAL_FLAG_SHORT,
                    EVAL_FLAG_LONG,
                    true,
                    "Groovy code to evaluate prior to startup in interactive mode")
            .addOption(
                    CLASSPATH_FLAG_SHORT, CLASSPATH_FLAG_LONG, true, "Additional locations to include on the classpath")
            .addOption(OptionBuilder.withLongOpt(BIND_FLAG_LONG)
                    .hasArgs(2)
                    .withDescription("Additional bindings to include in the cli")
                    .create(BIND_FLAG_SHORT));

    private static String[] additionalBindingsToSetUp = new String[] {};

    protected AtlasConsoleMain() {}

    public void run(String[] args) {
        try {
            CommandLineParser parser = new GnuParser();
            CommandLine cli = parser.parse(OPTIONS, args);
            if (cli.hasOption(HELP_FLAG_SHORT)) {
                usage();
                new HelpFormatter().printHelp("atlasDBConsole", OPTIONS, true);
                System.exit(0); // (authorized)
            }
            System.exit(execute(cli));
        } catch (ParseException e) {
            System.out.println("Invalid command line - " + e.getMessage()); // (authorized)
            new HelpFormatter().printHelp("atlasDBConsole", OPTIONS, true);
            System.exit(1); // (authorized)
        } catch (IOException e) {
            System.out.println("Invalid script file input - " + e.getMessage()); // (authorized)
            System.exit(1); // (authorized)
        }
    }

    protected int execute(CommandLine cli) throws CompilationFailedException, IOException {
        if (cli.hasOption(SCRIPT_FLAG_SHORT)) {
            evalFiles(cli.getArgs(), cli);
        } else {
            String setupScript = "-e//Starting AtlasConsole...please wait.\n" + ":set verbosity QUIET\n"
                    + ":set interpreterMode\n"
                    + ":set show-last-result false\n"
                    + getJavaCallbackString(cli.hasOption(MUTATIONS_ENABLED_FLAG_SHORT));

            if (cli.hasOption(BIND_FLAG_SHORT)) {
                additionalBindingsToSetUp = cli.getOptionValues(BIND_FLAG_SHORT);
                Preconditions.checkArgument(
                        additionalBindingsToSetUp.length % 2 == 0,
                        "An odd amount of parameters were passed into --bind");
            }

            if (cli.hasOption(EVAL_FLAG_SHORT)) {
                setupScript += "\n" + Joiner.on('\n').join(cli.getOptionValues(EVAL_FLAG_SHORT));
            }

            setupScript += "\n//AtlasConsole started, type help() for more info!";
            List<String> args = new ArrayList<String>(Arrays.asList(cli.getArgs()));
            args.add(setupScript);
            if (cli.hasOption(CLASSPATH_FLAG_SHORT)) {
                args.add("-cp");
                args.add(cli.getOptionValue(CLASSPATH_FLAG_SHORT));
            }
            String[] groovyArgs = args.toArray(new String[0]);
            Main.main(groovyArgs);
        }
        return 0;
    }

    protected void usage() {
        String prependMessage = "\n"
            + "AtlasConsole is a command line utility to view and modify an instance of AtlasDB.\n"
            + "In addition to the arguments listed below, the utility accepts a filepath to a Groovy script to"
            + " runprior to startup. Finally, the utility also accepts all arguments that the Groovysh utility takes."
            + " \n"
            + "See http://docs.groovy-lang.org/latest/html/documentation/#_groovysh_the_groovy_shell for details.";
        System.out.println(prependMessage); // (authorized)
    }

    private void evalFiles(String[] filepaths, CommandLine cli) throws CompilationFailedException, IOException {
        Binding binding = setupBinding(new Binding(), cli.hasOption(MUTATIONS_ENABLED_FLAG_SHORT));
        GroovyShell shell = new GroovyShell(binding);
        if (cli.hasOption(CLASSPATH_FLAG_SHORT)) {
            shell.getClassLoader().addClasspath(cli.getOptionValue(CLASSPATH_FLAG_SHORT));
        }
        for (String filepath : filepaths) {
            File file = new File(filepath);
            shell.evaluate(file);
        }
    }

    protected String getJavaCallbackString(boolean mutationsEnabled) {
        return "com.palantir.atlasdb.console.AtlasConsoleMain.callback(this, " + mutationsEnabled + ")";
    }

    public static void main(String[] args) {
        new AtlasConsoleMain().run(args);
    }

    private static Binding setupBinding(Binding binding, boolean mutationsEnabled) {
        for (int i = 0; i < additionalBindingsToSetUp.length; i += 2) {
            binding.setVariable(additionalBindingsToSetUp[i], additionalBindingsToSetUp[i + 1]);
        }

        AtlasConsoleService atlasConsoleService = new DisconnectedAtlasConsoleService();
        AtlasConsoleServiceWrapper atlasConsoleServiceWrapper = AtlasConsoleServiceWrapper.init(atlasConsoleService);
        return AtlasConsoleBinder.create(binding, new AtlasCoreModule(atlasConsoleServiceWrapper, mutationsEnabled));
    }

    /**
     * Note on the System.setSecurityManager call:
     *
     * The main method of the Groovysh class initializes a SecurityManager to
     * prevent the use of System.exit. The SecurityManager is not actually
     * necessary for Groovysh, so this change removes it to allow JDBC
     * access (removes the need for users to create an explicit ~/.java.policy
     * file).
     */
    public static void callback(Script script, boolean mutationsEnabled)
            throws CompilationFailedException, IOException {
        System.setSecurityManager(null);
        setupBinding(script.getBinding(), mutationsEnabled);
    }
}
