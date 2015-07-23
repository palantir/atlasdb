package com.palantir.atlasdb.shell;

import java.awt.HeadlessException;
import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.common.base.Strings;

public class AtlasShellCli {
    public static AtlasShellCli create(AtlasShellContextFactory atlasShellContextFactory) {
        AtlasShellRun atlasShellRun = new AtlasShellRun(atlasShellContextFactory);
        return new AtlasShellCli(atlasShellRun);
    }

    private final AtlasShellRun atlasShellRun;

    private AtlasShellCli(AtlasShellRun atlasShellRun) {
        this.atlasShellRun = atlasShellRun;
    }

    public void run(String[] args) {
        Options options = new Options();
        options.addOption(new Option("h", "help", false, "Prints help message."));
        options.addOption(new Option("headless", "Force headless (non-GUI) mode; unnecessary if a scriptlet was specified."));
        options.addOption(new Option(
                "scriptlet",
                true,
                "Ruby to execute at start; specifying a scriptlet automatically forces headless mode." +
                "  If the argument corresponds to a valid .rb file, then the contents of that" +
                " file will be executed.  Otherwise, the argument will be treated directly as Ruby code."));
        try {
            CommandLineParser parser = new GnuParser();
            CommandLine cli = parser.parse(options, args);
            if (cli.hasOption("h")) {
                System.out.println("AtlasDBShell is an interface for manually inspecting an AtlasDB\n" + // (authorized)
                        "instance (similar to squirrel or sqlplus for a SQL database).\n");
                new HelpFormatter().printHelp("atlasDBShell", options, true);
                System.exit(0); // (authorized)
            }

            String scriptlet = cli.getOptionValue("scriptlet", "");

            if (!scriptlet.isEmpty() && cli.hasOption("headless")) {
                System.out.println("Note: specifying a scriptlet automatically forces atlasShell to run in headless mode."); // (authorized)
            }

            if (!scriptlet.isEmpty()) {
                atlasShellRun.runHeadless(turnRubyCodeOrRubyFileIntoRubyCode(scriptlet));
            } else if (cli.hasOption("headless")) {
                atlasShellRun.runHeadless("");
            } else {
                atlasShellRun.runHeaded();
            }
        } catch (ParseException e) {
            System.out.println("Invalid command line - " + e.getMessage()); // (authorized)
            new HelpFormatter().printHelp("atlasDBShell", options, true);
            System.exit(1); // (authorized)
        } catch (HeadlessException e) {
            System.out.println("Error: The graphics enviroment is set to headless. Try running with the --headless option."); // (authorized)
            new HelpFormatter().printHelp("atlasDBShell", options, true);
            System.exit(1); // (authorized)
        }
    }


    /**
     * This implements something that I consider a misfeature introduced originally as a fix to
     * QA-80700, where you can pass either the name of a ruby file or the text of a ruby script to
     * the same command line argument, and it can be interpreted either way...
     */
    private String turnRubyCodeOrRubyFileIntoRubyCode(String scriptlet) {
        if (scriptlet.endsWith(".rb")) {
            File file = new File(scriptlet); // looks in the dispatchServer directory if running from the shell
            if (!file.exists()) {
                String atlasShellDir = System.getProperty("atlasdb-shell.dir");
                if (!Strings.isNullOrEmpty(atlasShellDir)) {
                    file = new File(atlasShellDir, scriptlet);
                }
                if (!file.exists()) {
                    throw new IllegalArgumentException("Warning: " + scriptlet + " looks like a Ruby file but doesn't exist on the file system."); // (authorized)
                }
            }
            String absoluteScriptletPath = file.getAbsolutePath();
            scriptlet = "load '" + absoluteScriptletPath + "'";
        }
        return scriptlet;
    }

    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "WARN");
        AtlasShellCli.create(new DefaultAtlasShellContextFactory()).run(args);
    }
}
