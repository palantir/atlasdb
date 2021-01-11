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
package com.palantir.atlasdb.table.description.render;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.stream.Collectors;

public class ImportRenderer extends Renderer {
    private static final ImmutableList<String> IMPORT_PREFIXES =
            ImmutableList.of("java.", "javax.", "org.", "com.", "net.");
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
        for (String prefix : IMPORT_PREFIXES) {
            boolean anyImportsRendered = renderImportsWithPrefix(prefix);
            if (anyImportsRendered) {
                line();
            }
        }
    }

    private boolean renderImportsWithPrefix(String prefix) {
        List<String> importClasses = importsSortedByFullName().stream()
                .filter(name -> name.startsWith(prefix))
                .collect(Collectors.toList());
        importClasses.forEach(importClass -> line("import ", importClass, ";"));
        return !importClasses.isEmpty();
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
