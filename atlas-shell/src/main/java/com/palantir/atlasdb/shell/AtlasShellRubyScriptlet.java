package com.palantir.atlasdb.shell;

import java.util.Map;

import com.google.common.collect.Maps;

/**
 * An AtlasShellRubyScriptlet encapsulates a Ruby script string, but it can do some preprocessing,
 * which is mostly used to hide passwords.
 */
public class AtlasShellRubyScriptlet {
    /**
     * @param rawScriptlet the unprocessed scriptlet
     */
    public AtlasShellRubyScriptlet(String rawScriptlet) {
        this.rawScriptlet = rawScriptlet;
    }

    private final String rawScriptlet;

    private Map<String, String> macroToValue = Maps.newHashMap();

    /**
     * @return the unprocessed scriptlet as passed to the constructor
     */
    public String getRawScriptlet() {
        return rawScriptlet;
    }

    /**
     * Add a macro substitution to the substitution table
     *
     * @param macro string to be replaced
     * @param value string it shall be replaced by
     */
    public void substitute(String macro, String value) {
        macroToValue.put(macro, value);
    }

    /**
     * Preprocess the unprocessed scriptlet according to the substitution table.
     *
     * @return the preprocessed scriptlet
     */
    public String preprocess() {
        String scriptlet = rawScriptlet;
        for (Map.Entry<String, String> macroAndValue : macroToValue.entrySet()) {
            String macro = macroAndValue.getKey();
            String value = macroAndValue.getValue();
            scriptlet = scriptlet.replaceAll(macro, value);
        }
        return scriptlet;
    }
}
