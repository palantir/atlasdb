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
import com.palantir.atlasdb.table.description.ValueType;

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
        line("public static final class ", Name, " implements Persistable, Comparable<", Name, "> {"); {
            fields();
            line();
            staticFactory();
            line();
            constructor();
            line();
            for (NameComponentDescription comp : getRowPartsWithoutHash()) {
                getVarName(comp);
                line();
            }
            for (NameComponentDescription comp : getRowPartsWithoutHash()) {
                getVarNameFun(comp);
                line();
            }
            if (getRowPartsWithoutHash().size() == 1) {
                fromVarNameFun();
                line();
            }
            persistToBytes();
            line();
            bytesHydrator();
            line();
            if (rangeScanAllowed) {
                int firstSortedIndex = desc.getRowParts().size() - 1;
                for ( ; firstSortedIndex > 0; firstSortedIndex--) {
                    if (!desc.getRowParts().get(firstSortedIndex).getType().supportsRangeScans()) {
                        break;
                    }
                }
                for (int i = 1; i <= desc.getRowParts().size() - 1; i++) {
                    createPrefixRange(i, firstSortedIndex < i);
                    line();
                    prefix(i, firstSortedIndex < i);
                    line();
                }
            }
            renderToString();
            line();
            renderEquals();
            line();
            renderHashCode();
            line();
            renderCompareTo();
        } line("}");
    }

    private void javaDoc() {
        line("/**");
        line(" * <pre>");
        line(" * ", Name, " {", "");
        for (NameComponentDescription comp : desc.getRowParts()) {
            boolean descending = comp.getOrder() == ValueByteOrder.DESCENDING;
            line(" *   {@literal ", descending ? "@Descending " : "",  TypeName(comp), " ", varName(comp), "};");
        }
        line(" * }", "");
        line(" * </pre>");
        line(" */");
    }

    private void fields() {
        for (NameComponentDescription comp : desc.getRowParts()) {
            line("private final ", typeName(comp), " ", varName(comp), ";");
        }
    }

    private void staticFactory() {
        line("public static ", Name, " of"); renderParameterList(getRowPartsWithoutHash()); lineEnd(" {"); {
            if (desc.hasFirstComponentHash()) {
                renderComputeFirstComponentHash();
            }
            line("return new ", Name); renderArgumentList(); lineEnd(";");
        } line("}");
    }

    private void constructor() {
        line("private ", Name); renderParameterList(); lineEnd(" {"); {
            for (NameComponentDescription comp : desc.getRowParts()) {
                line("this.", varName(comp), " = ", varName(comp), ";");
            }
        } line("}");
    }

    private void getVarName(NameComponentDescription comp) {
        line("public ", typeName(comp), " get", VarName(comp), "() {"); {
            line("return ", varName(comp), ";");
        } line("}");
    }

    private void getVarNameFun(NameComponentDescription comp) {
        line("public static Function<", Name, ", ", TypeName(comp), "> get", VarName(comp), "Fun() {"); {
            line("return new Function<", Name, ", ", TypeName(comp), ">() {"); {
                line("@Override");
                line("public ", TypeName(comp), " apply(", Name, " row) {"); {
                    line("return row.", varName(comp), ";");
                } line("}");
            } line("};");
        } line("}");
    }

    private void fromVarNameFun() {
        NameComponentDescription comp = Iterables.getOnlyElement(getRowPartsWithoutHash());
        line("public static Function<", TypeName(comp), ", ", Name, "> from", VarName(comp), "Fun() {"); {
            line("return new Function<", TypeName(comp), ", ", Name, ">() {"); {
                line("@Override");
                line("public ", Name, " apply(", TypeName(comp), " row) {"); {
                    line("return ", Name, ".of(row);");
                } line("}");
            } line("};");
        } line("}");
    }

    private void persistToBytes() {
        line("@Override");
        line("public byte[] persistToBytes() {"); {
            List<String> vars = Lists.newArrayList();
            for (NameComponentDescription comp : desc.getRowParts()) {
                String var = varName(comp) + "Bytes";
                vars.add(var);
                line("byte[] ", var, " = ", comp.getType().getPersistCode(varName(comp)), ";");
                if (comp.getOrder() == ValueByteOrder.DESCENDING) {
                    line("EncodingUtils.flipAllBitsInPlace(", var, ");");
                }
            }
            line("return EncodingUtils.add(", Joiner.on(", ").join(vars), ");");
        } line("}");
    }

    private void bytesHydrator() {
        line("public static final Hydrator<", Name, "> BYTES_HYDRATOR = new Hydrator<", Name, ">() {"); {
            line("@Override");
            line("public ", Name, " hydrateFromBytes(byte[] __input) {"); {
                line("int __index = 0;");
                List<String> vars = Lists.newArrayList();
                for (NameComponentDescription comp : desc.getRowParts()) {
                    String var = varName(comp);
                    vars.add(var);
                    if (comp.getOrder() == ValueByteOrder.ASCENDING) {
                        line(TypeName(comp), " ", var, " = ", comp.getType().getHydrateCode("__input", "__index"), ";");
                    } else {
                        line(TypeName(comp), " ", var, " = ", comp.getType().getFlippedHydrateCode("__input", "__index"), ";");
                    }
                    line("__index += ", comp.getType().getHydrateSizeCode(var), ";");
                }
                line("return new ", Name, "(", Joiner.on(", ").join(vars), ");");
            } line("}");
        } line("};");
    }

    private void createPrefixRange(int i, boolean isSorted) {
        List<NameComponentDescription> components = getRowPartsWithoutHash().subList(0, i);
        line("public static RangeRequest.Builder createPrefixRange", isSorted ? "" : "Unsorted"); renderParameterList(components); lineEnd(" {"); {
            List<String> vars = Lists.newArrayList();
            if (desc.hasFirstComponentHash()) {
                renderComputeFirstComponentHash();
                String var = NameMetadataDescription.HASH_ROW_COMPONENT_NAME + "Bytes";
                vars.add(var);
                line("byte[] ", var, " = ", ValueType.FIXED_LONG.getPersistCode(NameMetadataDescription.HASH_ROW_COMPONENT_NAME), ";");
            }
            for (NameComponentDescription comp : components) {
                String var = varName(comp) + "Bytes";
                vars.add(var);
                line("byte[] ", var, " = ", comp.getType().getPersistCode(varName(comp)), ";");
                if (comp.getOrder() == ValueByteOrder.DESCENDING) {
                    line("EncodingUtils.flipAllBitsInPlace(", var, ");");
                }
            }
            line("return RangeRequest.builder().prefixRange(EncodingUtils.add(", Joiner.on(", ").join(vars), "));");
        } line("}");
    }

    private void prefix(int i, boolean isSorted) {
        List<NameComponentDescription> components = getRowPartsWithoutHash().subList(0, i);
        line("public static Prefix prefix", isSorted ? "" : "Unsorted"); renderParameterList(components); lineEnd(" {"); {
            List<String> vars = Lists.newArrayList();
            if (desc.hasFirstComponentHash()) {
                renderComputeFirstComponentHash();
                String var = NameMetadataDescription.HASH_ROW_COMPONENT_NAME + "Bytes";
                vars.add(var);
                line("byte[] ", var, " = ", ValueType.FIXED_LONG.getPersistCode(NameMetadataDescription.HASH_ROW_COMPONENT_NAME), ";");
            }
            for (NameComponentDescription comp : components) {
                String var = varName(comp) + "Bytes";
                vars.add(var);
                line("byte[] ", var, " = ", comp.getType().getPersistCode(varName(comp)), ";");
                if (comp.getOrder() == ValueByteOrder.DESCENDING) {
                    line("EncodingUtils.flipAllBitsInPlace(", var, ");");
                }
            }
            line("return new Prefix(EncodingUtils.add(", Joiner.on(", ").join(vars), "));");
        } line("}");
    }

    private void renderComputeFirstComponentHash() {
        NameComponentDescription firstRowPart = getRowPartsWithoutHash().get(0);
        line("long ", NameMetadataDescription.HASH_ROW_COMPONENT_NAME, " = Hashing.murmur3_128().hashBytes(ValueType.", firstRowPart.getType().name(), ".convertFromJava(", varName(firstRowPart), ")).asLong();");
    }

    private void renderToString() {
        line("@Override");
        line("public String toString() {"); {
            line("return MoreObjects.toStringHelper(getClass().getSimpleName())");
            for (NameComponentDescription comp : desc.getRowParts()) {
                line("    .add(\"", varName(comp), "\", ", varName(comp), ")");
            }
            line("    .toString();");
        } line("}");
    }

    private void renderEquals() {
        line("@Override");
        line("public boolean equals(Object obj) {"); {
            line("if (this == obj) {"); {
                line("return true;");
            } line("}");
            line("if (obj == null) {"); {
                line("return false;");
            } line("}");
            line("if (getClass() != obj.getClass()) {"); {
                line("return false;");
            } line("}");
            line(Name, " other = (", Name, ") obj;");
            line("return");
            for (NameComponentDescription comp : desc.getRowParts()) {
                if (comp.getType() == ValueType.BLOB || comp.getType() == ValueType.SIZED_BLOB) {
                    lineEnd(" Arrays.equals(", varName(comp), ", other.", varName(comp), ") &&");
                } else {
                    lineEnd(" Objects.equal(", varName(comp), ", other.", varName(comp), ") &&");
                }
            }
            replace(" &&", ";");
        } line("}");
    }

    private void renderHashCode() {
        line("@Override");
        line("public int hashCode() {"); {
            renderHashCodeMethodCall();
        } line("}");
    }

    private void renderHashCodeMethodCall() {
        if (desc.getRowParts().size() > 1) {
            renderHashCodeMethodCall("return Arrays.deepHashCode(new Object[]{ ", " });");
        } else {
            renderHashCodeMethodCall("return Objects.hashCode(", ");");
        }
    }

    private void renderHashCodeMethodCall(String methodOpening, String methodClosing) {
        line(methodOpening);
        renderVariableList();
        replace(", ", methodClosing);
    }

    private void renderVariableList() {
        for (NameComponentDescription comp : desc.getRowParts()) {
            lineEnd(varName(comp), ", ");
        }
    }

    private void renderCompareTo() {
        line("@Override");
        line("public int compareTo(", Name, " o) {");
        {
            line("return ComparisonChain.start()");
            for (NameComponentDescription comp : desc.getRowParts()) {
                String comparator = TypeName(comp).equals("byte[]") ? ", UnsignedBytes.lexicographicalComparator()" : "";
                line("    .compare(this.", varName(comp), ", o.", varName(comp), comparator, ")");
            }
            line("    .result();");
        } line("}");
    }

    private void renderParameterList() {
        renderParameterList(desc.getRowParts());
    }

    private void renderParameterList(List<NameComponentDescription> components) {
        lineEnd("(");
        for (NameComponentDescription comp : components.subList(0, components.size())) {
            lineEnd(typeName(comp), " ", varName(comp), ", ");
        }
        replace(", ", ")");
    }

    private void renderArgumentList() {
        renderArgumentList(desc.getRowParts().size());
    }

    private void renderArgumentList(int i) {
        lineEnd("(");
        for (NameComponentDescription comp : desc.getRowParts().subList(0, i)) {
            lineEnd(varName(comp), ", ");
        }
        replace(", ", ")");
    }

    private List<NameComponentDescription> getRowPartsWithoutHash() {
        if (!desc.hasFirstComponentHash()) {
            return desc.getRowParts();
        } else {
            return desc.getRowParts().subList(1, desc.getRowParts().size());
        }
    }
}
