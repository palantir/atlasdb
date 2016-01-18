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
package com.palantir.atlasdb.table.description.render;

import java.util.Collection;
import java.util.SortedSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;

public class ImportRenderer extends Renderer {
    private final Collection<Class<?>> imports;

    public ImportRenderer(Renderer parent, Collection<Class<?>> imports) {
        super(parent);
        this.imports = imports;
    }

    @Override
    protected void run() {
        throw new UnsupportedOperationException();
    }

    void renderImports() {
        for (String prefix : ImmutableList.of("java.", "javax.", "org.", "com.")) {
            for (String importClass : importsSortedByFullName()) {
                if (importClass.startsWith(prefix)) {
                    line("import ", importClass, ";");
                }
            }
            line();
        }
    }

    void renderImportJavaDoc() {
        line("/**");
        line(" * This exists to avoid unused import warnings");
        for (String className : importsSortedBySimpleName()) {
            line(" * {@link ", className, "}", "");
        }
        line(" */");
    }

    private SortedSet<String> importsSortedByFullName() {
        ImmutableSortedSet.Builder<String> sortedImports = ImmutableSortedSet.naturalOrder();
        for (Class<?> clazz : imports) {
            sortedImports.add(clazz.getCanonicalName());
        }
        return sortedImports.build();
    }

    private SortedSet<String> importsSortedBySimpleName() {
        ImmutableSortedSet.Builder<String> sortedImports = ImmutableSortedSet.naturalOrder();
        for (Class<?> clazz : imports) {
            sortedImports.add(clazz.getSimpleName());
        }
        return sortedImports.build();
    }

}
