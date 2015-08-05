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
package com.palantir.atlasdb.shell;

import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.ext.Readline;
import org.jruby.runtime.Arity;
import org.jruby.runtime.Block;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.callback.Callback;

import com.google.common.collect.Lists;

import jline.Completor;

// Copying ProcCompletor, the standard tab completor, because there seems to be no way to extend it.
public class BiasedProcCompletor implements Completor {

    IRubyObject procCompletor;
    //\t\n\"\\'`@$><=;|&{(
    static private String[] delimiters = {" ", "\t", "\n", "\"", "\\", "'", "`", "@", "$", ">", "<", "=", ";", "|", "&", "{", "("};

    public BiasedProcCompletor(IRubyObject procCompletor) {
        this.procCompletor = procCompletor;
    }

    public static String getDelimiter() {
        StringBuilder result = new StringBuilder(delimiters.length);
        for (String delimiter : delimiters) {
            result.append(delimiter);
        }
        return result.toString();
    }

    public static void setDelimiter(String delimiter) {
        List<String> l = new ArrayList<String>();
        CharBuffer buf = CharBuffer.wrap(delimiter);
        while (buf.hasRemaining()) {
            l.add(String.valueOf(buf.get()));
        }
        delimiters = l.toArray(new String[l.size()]);
    }

    private int wordIndexOf(String buffer) {
        int index = 0;
        for (String c : delimiters) {
            index = buffer.lastIndexOf(c);
            if (index != -1) return index;
        }
        return index;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public int complete(String buffer, int cursor, List candidates) {
        buffer = buffer.substring(0, cursor);
        int index = wordIndexOf(buffer);
        if (index != -1) buffer = buffer.substring(index + 1);

        Ruby runtime = procCompletor.getRuntime();
        ThreadContext context = runtime.getCurrentContext();
        IRubyObject result = procCompletor.callMethod(context, "call", runtime.newString(buffer));
        IRubyObject comps = result.callMethod(context, "to_a");

        List<String> atlasShellCandidates = Lists.newArrayList();
        List<String> otherCandidates = Lists.newArrayList();

        List<String> atlasShellMethods = Lists.newArrayList();
        RubyModule atlasDBShellModule = runtime.getModule("AtlasDBShell");
        if (atlasDBShellModule != null) {
            atlasShellMethods.addAll(atlasDBShellModule.getMethods().keySet());
            for (IRubyObject c : atlasDBShellModule.getConstantMap().values()) {
                if (c instanceof RubyClass) {
                    atlasShellMethods.addAll(((RubyClass) c).getMethods().keySet());
                }
            }
        }

        Set<String> varNames = runtime.getGlobalVariables().getNames();
        for (String varName : varNames) {
            if (varName.startsWith("$" + buffer)) {
                atlasShellCandidates.add(varName.substring(1));
            }
        }

        if (comps instanceof List) {
            for (Iterator i = ((List) comps).iterator(); i.hasNext();) {
                Object obj = i.next();
                if (obj != null) {
                    int methodIndex = obj.toString().lastIndexOf('.');
                    if (atlasShellMethods.contains(obj.toString().substring(methodIndex + 1).toLowerCase())) {
                        atlasShellCandidates.add(obj.toString());
                    } else {
                        otherCandidates.add(obj.toString());
                    }
                }
            }
            Collections.sort(atlasShellCandidates);
            Collections.sort(otherCandidates);
            candidates.addAll(atlasShellCandidates);
            candidates.addAll(otherCandidates);
        }

        return cursor - buffer.length();
    }

    // Copying TextAreaReadline to hack in our own tab completor.
    public static void hookIntoRuntime(final Ruby runtime) {
        /* Hack in to replace usual readline with this */
        runtime.getLoadService().require("readline");
        RubyModule readlineM = runtime.fastGetModule("Readline");

        readlineM.defineModuleFunction("completion_proc=", new Callback() {
            @Override
            public IRubyObject execute(IRubyObject recv, IRubyObject[] args, Block block) {
                IRubyObject proc = args[0];
                if (!proc.respondsTo("call")) {
                    throw recv.getRuntime().newArgumentError("argument must respond to call");
                }
                Readline.setCompletor(Readline.getHolder(recv.getRuntime()),
                        new BiasedProcCompletor(proc));
                return recv.getRuntime().getNil();
            }

            @Override
            public Arity getArity() { return Arity.twoArguments(); }
        });
    }
}
