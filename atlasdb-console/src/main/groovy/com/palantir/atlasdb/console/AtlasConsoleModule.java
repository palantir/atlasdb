package com.palantir.atlasdb.console;

import java.util.Map;

import groovy.lang.Closure;

public interface AtlasConsoleModule {
    /**
     * returns a map from topic to help text
     */
    Map<String, String> getHelp();

    /**
     * returns a map from variable name to binding in the console
     */
    Map<String, Closure> getBindings();
}
