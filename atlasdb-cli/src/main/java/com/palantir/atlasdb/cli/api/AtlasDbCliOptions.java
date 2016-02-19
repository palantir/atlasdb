package com.palantir.atlasdb.cli.api;

import com.lexicalscope.jewel.cli.Option;

public interface AtlasDbCliOptions {

    @Option(helpRequest = true)
    boolean getHelp();

    @Option(longName = "config", shortName = "c")
    String getConfigFileName();

}
