/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
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
