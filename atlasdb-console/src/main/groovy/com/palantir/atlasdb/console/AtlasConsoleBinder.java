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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.console.annotations.ConsoleBinding;
import groovy.lang.Binding;
import groovy.lang.Closure;
import java.lang.reflect.Method;
import java.util.Map;
import org.codehaus.groovy.runtime.MethodClosure;

public final class AtlasConsoleBinder {

    private AtlasConsoleBinder() {
        // uninstantiable
    }

    public static Binding create(Object... modules) {
        return create(new Binding(), modules);
    }

    public static Binding create(Binding binding, Object... modules) {
        Map<String, String> topicToHelpMap = Maps.newHashMap();
        for (Object module : modules) {
            AtlasConsoleModule m = createModule(module);
            for (Map.Entry<String, Closure> e : m.getBindings().entrySet()) {
                binding.setVariable(e.getKey(), e.getValue());
            }
            topicToHelpMap.putAll(m.getHelp());
        }
        binding.setVariable("help", new HelpClosure(binding, topicToHelpMap));
        return binding;
    }

    private static AtlasConsoleModule createModule(final Object module) {
        if (module instanceof AtlasConsoleModule) {
            return (AtlasConsoleModule) module;
        }

        final Map<String, String> help = Maps.newHashMap();
        final Map<String, Closure> bindings = Maps.newHashMap();
        for (Method m : module.getClass().getMethods()) {
            ConsoleBinding b = m.getAnnotation(ConsoleBinding.class);
            if (b != null) {
                Preconditions.checkArgument(
                        !Strings.isNullOrEmpty(b.name()),
                        "@ConsoleBinding name cannot be null or empty on %s.:",
                        m.getName());
                bindings.put(b.name(), createClosure(module, m));
                if (!Strings.isNullOrEmpty(b.help())) {
                    help.put(b.name(), b.help());
                }
            }
        }
        Preconditions.checkArgument(!bindings.isEmpty(), "%s has not @ConsoleBinding annotations", module.getClass());
        return new AtlasConsoleModule() {
            @Override
            public Map<String, String> getHelp() {
                return help;
            }

            @Override
            public Map<String, Closure> getBindings() {
                return bindings;
            }
        };
    }

    private static Closure createClosure(Object module, Method m) {
        return new MethodClosure(module, m.getName());
    }

    public static class HelpClosure extends Closure {
        private final Map<String, String> help;

        public HelpClosure(Object owner, Map<String, String> help) {
            super(owner, null);
            this.help = help;
        }

        @Override
        public Object call() {
            System.out.println("\nTopics with help messages:\n"); // (authorized)
            System.out.println(help.keySet().toString()); // (authorized)
            System.out.println("\nCall help(\'topic\') to view information on a specific topic.\n"); // (authorized)
            System.out.println("If you'd like to enable database mutations (not recommended), re-run console with the"
                    + " 'mutations_enabled' flag\n"); // (authorized)
            return null;
        }

        @Override
        public Object call(Object param) {
            Preconditions.checkArgument(param instanceof String);
            String topic = (String) param;
            if (help.containsKey(topic)) {
                System.out.println("\n" + help.get(topic) + "\n"); // (authorized)
            } else {
                System.out.println("\nNo help topics found for " + topic + ".\n"); // (authorized)
                System.out.println("Call help(\'topic\') to view information on a topic, " + // (authorized)
                        "or help() to view a list of topics with help messages.\n"); // (authorized)
            }
            return null;
        }
    }
}
