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
package com.palantir.atlasdb.table.description.render;

import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.table.description.TableDefinition;

public class TableFactoryRenderer {
    private final String schemaName;
    private final String packageName;
    private final SortedMap<String, TableDefinition> definitions;

    public TableFactoryRenderer(String schemaName,
                                String packageName,
                                Map<String, TableDefinition> definitions) {
        this.schemaName = schemaName;
        this.packageName = packageName;
        this.definitions = Maps.newTreeMap();
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
                _();
                _("public class ", TableFactory, " {"); {
                    _("private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;");
                    _();
                    constructors();
                    _();
                    for (Entry<String, TableDefinition> entry : definitions.entrySet()) {
                        getTable(entry.getKey(), entry.getValue());
                        _();
                    }
                    sharedTriggers();
                    _();
                    nullSharedTriggers();
                } _("}");
            }

            private void packageAndImports() {
                _("package ", packageName, ";");
                _();
                _("import java.util.List;");
                _();
                _("import com.google.common.base.Function;");
                _("import com.google.common.collect.ImmutableList;");
                _("import com.google.common.collect.Multimap;");
                _("import com.palantir.atlasdb.table.generation.Triggers;");
                _("import com.palantir.atlasdb.transaction.api.Transaction;");
            }

            private void constructors() {
                _("public static ", TableFactory, " of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {"); {
                    _("return new ", TableFactory, "(sharedTriggers);");
                } _("}");
                _();
                _("private ", TableFactory, "(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {"); {
                    _("this.sharedTriggers = sharedTriggers;");
                } _("}");
                _();
                _("public static ", TableFactory, " of() {"); {
                    _("return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of());");
                } _("}");
            }

            private void getTable(String name, TableDefinition table) {
                String Table = name + "Table";
                String Trigger = Table + "." + name + "Trigger";
                if (table.getGenericTableName() != null) {
                    _("public ", Table, " get", Table, "(Transaction t, String name, ", Trigger, "... triggers) {"); {
                        _("return ", Table, ".of(t, name, Triggers.getAllTriggers(t, sharedTriggers, triggers));");
                    } _("}");
                } else {
                    _("public ", Table, " get", Table, "(Transaction t, ", Trigger, "... triggers) {"); {
                        _("return ", Table, ".of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));");
                    } _("}");
                }
            }

            private void sharedTriggers() {
                _("public interface SharedTriggers extends");
                for (String name : definitions.keySet()) {
                    _("        ", name, "Table.", name, "Trigger,");
                }
                replace(",", " {"); {
                    _("/* empty */");
                } _("}");
            }

            private void nullSharedTriggers() {
                _("public abstract static class NullSharedTriggers implements SharedTriggers {"); {
                    for (Entry<String, TableDefinition> entry : definitions.entrySet()) {
                        String name = entry.getKey();
                        TableDefinition table = entry.getValue();
                        String Table = name + "Table";
                        String Row = Table + "." + name + "Row";
                        String ColumnValue = Table + "." + name + (table.toTableMetadata().getColumns().hasDynamicColumns() ? "ColumnValue" : "NamedColumnValue<?>");
                        _("@Override");
                        _("public void put", name, "(Multimap<", Row, ", ? extends ", ColumnValue, "> newRows) {"); {
                            _("// do nothing");
                        } _("}");
                        _();
                    }
                    strip("\n");
                } _("}");
            }
        }.render();
    }
}
