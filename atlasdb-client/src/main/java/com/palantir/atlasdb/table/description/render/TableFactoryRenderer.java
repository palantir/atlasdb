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

import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.description.TableDefinition;

public class TableFactoryRenderer {
    private final String schemaName;
    private final String packageName;
    private final String defaultNamespace;
    private final SortedMap<String, TableDefinition> definitions;

    public TableFactoryRenderer(String schemaName,
                                String packageName,
                                Namespace defaultNamespace,
                                Map<String, TableDefinition> definitions) {
        this.schemaName = schemaName;
        this.packageName = packageName;
        this.definitions = Maps.newTreeMap();
        this.defaultNamespace = defaultNamespace.getName();
        for (Entry<String, TableDefinition> entry : definitions.entrySet()) {
            this.definitions.put(Renderers.getClassTableName(entry.getKey(), entry.getValue()), entry.getValue());
        }
    }

    public String getPackageName() {
        return packageName;
    }

    public String getClassName() {
        return schemaName + "TableFactory";
    }

    public String render() {
        final String TableFactory = getClassName();

        return new Renderer() {
            @Override
            protected void run() {
                packageAndImports();
                line();
                line("@Generated(\"",  TableFactoryRenderer.class.getName(), "\")");
                line("public final class ", TableFactory, " {"); {
                    if (defaultNamespace.isEmpty()) {
                        line("private final static Namespace defaultNamespace = Namespace.EMPTY_NAMESPACE;");
                    } else {
                        line("private final static Namespace defaultNamespace = Namespace.create(\"" + defaultNamespace + "\", Namespace.UNCHECKED_NAME);");
                    }
                    line("private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;");
                    line("private final Namespace namespace;");
                    line();
                    constructors();
                    line();
                    for (Entry<String, TableDefinition> entry : definitions.entrySet()) {
                        getTable(entry.getKey(), entry.getValue());
                        line();
                    }
                    sharedTriggers();
                    line();
                    nullSharedTriggers();
                } line("}");
            }

            private void packageAndImports() {
                line("package ", packageName, ";");
                line();
                line("import java.util.List;");
                line();
                line("import javax.annotation.Generated;");
                line();
                line("import com.google.common.base.Function;");
                line("import com.google.common.collect.ImmutableList;");
                line("import com.google.common.collect.Multimap;");
                line("import com.palantir.atlasdb.keyvalue.api.Namespace;");
                line("import com.palantir.atlasdb.table.generation.Triggers;");
                line("import com.palantir.atlasdb.transaction.api.Transaction;");
            }

            private void constructors() {
                line("public static ", TableFactory, " of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {"); {
                    line("return new ", TableFactory, "(sharedTriggers, namespace);");
                } line("}");
                line();
                line("public static ", TableFactory, " of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {"); {
                    line("return new ", TableFactory, "(sharedTriggers, defaultNamespace);");
                } line("}");
                line();
                line("private ", TableFactory, "(List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {"); {
                    line("this.sharedTriggers = sharedTriggers;");
                    line("this.namespace = namespace;");
                } line("}");
                line();
                line("public static ", TableFactory, " of(Namespace namespace) {"); {
                    line("return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), namespace);");
                } line("}");
                line();
                line("public static ", TableFactory, " of() {"); {
                    line("return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), defaultNamespace);");
                } line("}");
            }

            private void getTable(String name, TableDefinition table) {
                String Table = name + "Table";
                String Trigger = Table + "." + name + "Trigger";
                if (table.getGenericTableName() != null) {
                    line("public ", Table, " get", Table, "(Transaction t, String name, ", Trigger, "... triggers) {"); {
                        line("return ", Table, ".of(t, namespace, name, Triggers.getAllTriggers(t, sharedTriggers, triggers));");
                    } line("}");
                } else {
                    line("public ", Table, " get", Table, "(Transaction t, ", Trigger, "... triggers) {"); {
                        line("return ", Table, ".of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));");
                    } line("}");
                }
            }

            private void sharedTriggers() {
                line("public interface SharedTriggers extends");
                for (String name : definitions.keySet()) {
                    line("        ", name, "Table.", name, "Trigger,");
                }
                replace(",", " {"); {
                    line("/* empty */");
                } line("}");
            }

            private void nullSharedTriggers() {
                line("public abstract static class NullSharedTriggers implements SharedTriggers {"); {
                    for (Entry<String, TableDefinition> entry : definitions.entrySet()) {
                        String name = entry.getKey();
                        TableDefinition table = entry.getValue();
                        String Table = name + "Table";
                        String Row = Table + "." + name + "Row";
                        String ColumnValue = Table + "." + name + (table.toTableMetadata().getColumns().hasDynamicColumns() ? "ColumnValue" : "NamedColumnValue<?>");
                        line("@Override");
                        line("public void put", name, "(Multimap<", Row, ", ? extends ", ColumnValue, "> newRows) {"); {
                            line("// do nothing");
                        } line("}");
                        line();
                    }
                    strip("\n");
                } line("}");
            }
        }.render();
    }
}
