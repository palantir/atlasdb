// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.table.description.render;

import static com.palantir.atlasdb.table.description.render.ComponentRenderers.TypeName;
import static com.palantir.atlasdb.table.description.render.ComponentRenderers.VarName;
import static com.palantir.atlasdb.table.description.render.ComponentRenderers.typeName;
import static com.palantir.atlasdb.table.description.render.ComponentRenderers.varName;

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;

class RowOrDynamicColumnRenderer extends Renderer {
    private final String Name;
    private final NameMetadataDescription desc;
    private final boolean rangeScanAllowed;

    public RowOrDynamicColumnRenderer(Renderer parent, String Name, NameMetadataDescription desc, boolean rangeScanAllowed) {
        super(parent);
        this.Name = Name;
        this.desc = desc;
        this.rangeScanAllowed = rangeScanAllowed;
    }

    @Override
    protected void run() {
        javaDoc();
        _("public static final class ", Name, " implements Persistable, Comparable<", Name, "> {"); {
            fields();
            _();
            staticFactory();
            _();
            constructor();
            _();
            for (NameComponentDescription comp : desc.getRowParts()) {
                getVarName(comp);
                _();
            }
            for (NameComponentDescription comp : desc.getRowParts()) {
                getVarNameFun(comp);
                _();
            }
            if (desc.getRowParts().size() == 1) {
                fromVarNameFun();
                _();
            }
            persistToBytes();
            _();
            bytesHydrator();
            _();
            if (rangeScanAllowed) {
                int firstSortedIndex = desc.getRowParts().size() - 1;
                for ( ; firstSortedIndex > 0; firstSortedIndex--) {
                    if (!desc.getRowParts().get(firstSortedIndex).getType().supportsRangeScans()) {
                        break;
                    }
                }
                for (int i = 1; i <= desc.getRowParts().size() - 1; i++) {
                    createPrefixRange(i, firstSortedIndex < i);
                    _();
                    prefix(i, firstSortedIndex < i);
                    _();
                }
            }
            renderToString();
            _();
            renderEquals();
            _();
            renderHashCode();
            _();
            renderCompareTo();
        } _("}");
    }

    private void javaDoc() {
        _("/**");
        _(" * <pre>");
        _(" * ", Name, " {", "");
        for (NameComponentDescription comp : desc.getRowParts()) {
            boolean descending = comp.getOrder() == ValueByteOrder.DESCENDING;
            _(" *   {@literal ", descending ? "@Descending " : "",  TypeName(comp), " ", varName(comp), "};");
        }
        _(" * }", "");
        _(" * </pre>");
        _(" */");
    }

    private void fields() {
        for (NameComponentDescription comp : desc.getRowParts()) {
            _("private final ", typeName(comp), " ", varName(comp), ";");
        }
    }

    private void staticFactory() {
        _("public static ", Name, " of"); renderParameterList(); __(" {"); {
            _("return new ", Name); renderArgumentList(); __(";");
        } _("}");
    }

    private void constructor() {
        _("private ", Name); renderParameterList(); __(" {"); {
            for (NameComponentDescription comp : desc.getRowParts()) {
                _("this.", varName(comp), " = ", varName(comp), ";");
            }
        } _("}");
    }

    private void getVarName(NameComponentDescription comp) {
        _("public ", typeName(comp), " get", VarName(comp), "() {"); {
            _("return ", varName(comp), ";");
        } _("}");
    }

    private void getVarNameFun(NameComponentDescription comp) {
        _("public static Function<", Name, ", ", TypeName(comp), "> get", VarName(comp), "Fun() {"); {
            _("return new Function<", Name, ", ", TypeName(comp), ">() {"); {
                _("@Override");
                _("public ", TypeName(comp), " apply(", Name, " row) {"); {
                    _("return row.", varName(comp), ";");
                } _("}");
            } _("};");
        } _("}");
    }

    private void fromVarNameFun() {
        NameComponentDescription comp = Iterables.getOnlyElement(desc.getRowParts());
        _("public static Function<", TypeName(comp), ", ", Name, "> from", VarName(comp), "Fun() {"); {
            _("return new Function<", TypeName(comp), ", ", Name, ">() {"); {
                _("@Override");
                _("public ", Name, " apply(", TypeName(comp), " row) {"); {
                    _("return new ", Name, "(row);");
                } _("}");
            } _("};");
        } _("}");
    }

    private void persistToBytes() {
        _("@Override");
        _("public byte[] persistToBytes() {"); {
            List<String> vars = Lists.newArrayList();
            for (NameComponentDescription comp : desc.getRowParts()) {
                String var = varName(comp) + "Bytes";
                vars.add(var);
                _("byte[] ", var, " = ", comp.getType().getPersistCode(varName(comp)), ";");
                if (comp.getOrder() == ValueByteOrder.DESCENDING) {
                    _("EncodingUtils.flipAllBitsInPlace(", var, ");");
                }
            }
            _("return EncodingUtils.add(", Joiner.on(", ").join(vars), ");");
        } _("}");
    }

    private void bytesHydrator() {
        _("public static final Hydrator<", Name, "> BYTES_HYDRATOR = new Hydrator<", Name, ">() {"); {
            _("@Override");
            _("public ", Name, " hydrateFromBytes(byte[] __input) {"); {
                _("int __index = 0;");
                List<String> vars = Lists.newArrayList();
                for (NameComponentDescription comp : desc.getRowParts()) {
                    String var = varName(comp);
                    vars.add(var);
                    if (comp.getOrder() == ValueByteOrder.ASCENDING) {
                        _(TypeName(comp), " ", var, " = ", comp.getType().getHydrateCode("__input", "__index"), ";");
                    } else {
                        _(TypeName(comp), " ", var, " = ", comp.getType().getFlippedHydrateCode("__input", "__index"), ";");
                    }
                    _("__index += ", comp.getType().getHydrateSizeCode(var), ";");
                }
                _("return of(", Joiner.on(", ").join(vars), ");");
            } _("}");
        } _("};");
    }

    private void createPrefixRange(int i, boolean isSorted) {
        _("public static RangeRequest.Builder createPrefixRange", isSorted ? "" : "Unsorted"); renderParameterList(i); __(" {"); {
            List<String> vars = Lists.newArrayList();
            for (NameComponentDescription comp : desc.getRowParts().subList(0, i)) {
                String var = varName(comp) + "Bytes";
                vars.add(var);
                _("byte[] ", var, " = ", comp.getType().getPersistCode(varName(comp)), ";");
                if (comp.getOrder() == ValueByteOrder.DESCENDING) {
                    _("EncodingUtils.flipAllBitsInPlace(", var, ");");
                }
            }
            _("return RangeRequest.builder().prefixRange(EncodingUtils.add(", Joiner.on(", ").join(vars), "));");
        } _("}");
    }

    private void prefix(int i, boolean isSorted) {
        _("public static Prefix prefix", isSorted ? "" : "Unsorted"); renderParameterList(i); __(" {"); {
            List<String> vars = Lists.newArrayList();
            for (NameComponentDescription comp : desc.getRowParts().subList(0, i)) {
                String var = varName(comp) + "Bytes";
                vars.add(var);
                _("byte[] ", var, " = ", comp.getType().getPersistCode(varName(comp)), ";");
                if (comp.getOrder() == ValueByteOrder.DESCENDING) {
                    _("EncodingUtils.flipAllBitsInPlace(", var, ");");
                }
            }
            _("return new Prefix(EncodingUtils.add(", Joiner.on(", ").join(vars), "));");
        } _("}");
    }

    private void renderToString() {
        _("@Override");
        _("public String toString() {"); {
            _("return MoreObjects.toStringHelper(this)");
            for (NameComponentDescription comp : desc.getRowParts()) {
                _("    .add(\"", varName(comp), "\", ", varName(comp), ")");
            }
            _("    .toString();");
        } _("}");
    }

    private void renderEquals() {
        _("@Override");
        _("public boolean equals(Object obj) {"); {
            _("if (this == obj) {"); {
                _("return true;");
            } _("}");
            _("if (obj == null) {"); {
                _("return false;");
            } _("}");
            _("if (getClass() != obj.getClass()) {"); {
                _("return false;");
            } _("}");
            _(Name, " other = (", Name, ") obj;");
            _("return");
            for (NameComponentDescription comp : desc.getRowParts()) {
                __(" Objects.equal(", varName(comp), ", other.", varName(comp), ") &&");
            }
            replace(" &&", ";");
        } _("}");
    }

    private void renderHashCode() {
        _("@Override");
        _("public int hashCode() {"); {
            _("return Objects.hashCode(");
            for (NameComponentDescription comp : desc.getRowParts()) {
                __(varName(comp), ", ");
            }
            replace(", ", ");");
        } _("}");
    }

    private void renderCompareTo() {
        _("@Override");
        _("public int compareTo(", Name, " o) {"); {
            _("return ComparisonChain.start()");
            for (NameComponentDescription comp : desc.getRowParts()) {
                String comparator = TypeName(comp).equals("byte[]") ? ", UnsignedBytes.lexicographicalComparator()" : "";
                _("    .compare(this.", varName(comp), ", o.", varName(comp), comparator, ")");
            }
            _("    .result();");
        } _("}");
    }

    private void renderParameterList() {
        renderParameterList(desc.getRowParts().size());
    }

    private void renderParameterList(int i) {
        __("(");
        for (NameComponentDescription comp : desc.getRowParts().subList(0, i)) {
            __(typeName(comp), " ", varName(comp), ", ");
        }
        replace(", ", ")");
    }

    private void renderArgumentList() {
        renderArgumentList(desc.getRowParts().size());
    }

    private void renderArgumentList(int i) {
        __("(");
        for (NameComponentDescription comp : desc.getRowParts().subList(0, i)) {
            __(varName(comp), ", ");
        }
        replace(", ", ")");
    }
}
