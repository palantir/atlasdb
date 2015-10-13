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
package com.palantir.atlasdb.table.description;

import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.GeneratedMessage;
import com.palantir.atlasdb.persist.api.Persister;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;
import com.palantir.atlasdb.table.description.ColumnValueDescription.Compression;
import com.palantir.atlasdb.table.description.constraints.ConstraintMetadata;
import com.palantir.atlasdb.table.description.constraints.ForeignKeyConstraintMetadata;
import com.palantir.atlasdb.table.description.constraints.RowConstraintMetadata;
import com.palantir.atlasdb.table.description.constraints.TableConstraint;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.common.persist.Persistable;

/**
 * Defines a table for a schema.
 *
 * Can be thought of as a builder for {@link DefaultTableMetadata} objects.
 */
public class CodeGeneratingTableDefinition extends AbstractDefinition implements TableDefinition {

    @Override
    protected ConflictHandler defaultConflictHandler() {
        return ConflictHandler.RETRY_ON_WRITE_WRITE;
    }

    public void rowName() {
        state = State.DEFINING_ROW_NAME;
    }

    public void columns() {
        state = State.DEFINING_COLUMNS;
    }

    public void dynamicColumns() {
        state = State.DEFINING_DYNAMIC_COLUMN;
    }

    public void constraints() {
        state = State.DEFINING_CONSTRAINTS;
    }

    public void column(String columnName, String shortName, Class<?> protoOrPersistable) {
        column(columnName, shortName, protoOrPersistable, Compression.NONE);
    }

    public void column(String columnName, String shortName, Class<?> protoOrPersistable, Compression compression) {
        Preconditions.checkState(state == State.DEFINING_COLUMNS);
        Preconditions.checkState(!noColumns);
        checkUniqueColumnNames(columnName, shortName);
        fixedColumns.add(new NamedColumnDescription(shortName, columnName, getColumnValueDescription(protoOrPersistable, compression)));
    }

    public void column(String columnName, String shortName, ValueType valueType) {
        Preconditions.checkState(state == State.DEFINING_COLUMNS);
        Preconditions.checkState(!noColumns);
        checkUniqueColumnNames(columnName, shortName);
        fixedColumns.add(new NamedColumnDescription(shortName, columnName, ColumnValueDescription.forType(valueType)));
    }

    public void noColumns() {
        Preconditions.checkState(state != State.DEFINING_COLUMNS);
        Preconditions.checkState(fixedColumns.isEmpty());
        fixedColumns.add(new NamedColumnDescription("e", "exists", ColumnValueDescription.forType(ValueType.VAR_LONG)));
        noColumns = true;
    }

    private void checkUniqueColumnNames(String columnName, String shortName) {
        Preconditions.checkState(!fixedColumnShortNames.contains(shortName));
        Preconditions.checkState(!fixedColumnLongNames.contains(columnName));
        fixedColumnShortNames.add(shortName);
        fixedColumnLongNames.add(columnName);
    }

    public void rowComponent(String componentName, ValueType valueType) {
        rowComponent(componentName, valueType, ValueByteOrder.ASCENDING);
    }

    public void rowComponent(String componentName, ValueType valueType, ValueByteOrder valueByteOrder) {
        Preconditions.checkState(state == State.DEFINING_ROW_NAME);
        rowNameComponents.add(new NameComponentDescription(componentName, valueType, valueByteOrder));
    }

    /**
     * Passing an empty list of partitioners means that this type is not able to be partitioned. This is sometimes
     * needed if the value type doesn't support the {@link UniformRowNamePartitioner}.  If the first entry in the table
     * cannot be partitioned then it is likely this table will cause hot spots.  Consider adding
     * partitionStrategy(HASH) in this case which will allow the DB to break it up however it wants.
     * <p>
     * If no partition() is specified the default is to use a {@link UniformRowNamePartitioner}
     */
    public void partition(RowNamePartitioner... partitioners) {
        Preconditions.checkState(state == State.DEFINING_ROW_NAME);
        NameComponentDescription last = rowNameComponents.get(rowNameComponents.size()-1);
        rowNameComponents.set(rowNameComponents.size()-1, last.withPartitioners(partitioners));
    }

    public ExplicitRowNamePartitioner explicit(String... componentValues) {
        Preconditions.checkState(state == State.DEFINING_ROW_NAME);
        return new ExplicitRowNamePartitioner(rowNameComponents.get(rowNameComponents.size()-1).getType(), ImmutableSet.copyOf(componentValues));
    }

    public ExplicitRowNamePartitioner explicit(long... componentValues) {
        Preconditions.checkState(state == State.DEFINING_ROW_NAME);
        Set<String> set = Sets.newHashSet();
        for (long l : componentValues) {
            set.add(Long.toString(l));
        }
        return new ExplicitRowNamePartitioner(rowNameComponents.get(rowNameComponents.size()-1).getType(), set);
    }

    public UniformRowNamePartitioner uniform() {
        Preconditions.checkState(state == State.DEFINING_ROW_NAME);
        return new UniformRowNamePartitioner(rowNameComponents.get(rowNameComponents.size()-1).getType());
    }

    public void columnComponent(String componentName, ValueType valueType) {
        columnComponent(componentName, valueType, ValueByteOrder.ASCENDING);
    }

    public void columnComponent(String componentName, ValueType valueType, ValueByteOrder valueByteOrder) {
        Preconditions.checkState(state == State.DEFINING_DYNAMIC_COLUMN);
        dynamicColumnNameComponents.add(new NameComponentDescription(componentName, valueType, valueByteOrder));
    }

    public void value(Class<?> protoOrPersistable) {
        value(protoOrPersistable, Compression.NONE);
    }

    public void value(Class<?> protoOrPersistable, Compression compression) {
        Preconditions.checkState(state == State.DEFINING_DYNAMIC_COLUMN);
        dynamicColumnValue = getColumnValueDescription(protoOrPersistable, compression);
    }

    public void value(ValueType valueType) {
        Preconditions.checkState(state == State.DEFINING_DYNAMIC_COLUMN);
        dynamicColumnValue = ColumnValueDescription.forType(valueType);
        if (maxValueSize == Integer.MAX_VALUE) {
            maxValueSize = valueType.getMaxValueSize();
        }
    }

    public void tableConstraint(TableConstraint constraint) {
        Preconditions.checkState(state == State.DEFINING_CONSTRAINTS);
        constraintBuilder.addTableConstraint(constraint);
    }

    public void rowConstraint(RowConstraintMetadata constraint) {
        Preconditions.checkState(state == State.DEFINING_CONSTRAINTS);
        constraintBuilder.addRowConstraint(constraint);
    }

    public void foreignKeyConstraint(ForeignKeyConstraintMetadata constraint) {
        Preconditions.checkState(state == State.DEFINING_CONSTRAINTS);
        constraintBuilder.addForeignKeyConstraint(constraint);
    }

    public void dbCompressionRequested(){
        dbCompressionRequested = true;
    }

    /* (non-Javadoc)
	 * @see com.palantir.atlasdb.table.description.TableDefinition#isDbCompressionRequested()
	 */
    @Override
	public boolean isDbCompressionRequested(){
        return dbCompressionRequested;
    }

    public void rangeScanAllowed() {
        rangeScanAllowed = true;
    }

    /* (non-Javadoc)
	 * @see com.palantir.atlasdb.table.description.TableDefinition#isRangeScanAllowed()
	 */
    @Override
	public boolean isRangeScanAllowed() {
        return rangeScanAllowed;
    }

    public void negativeLookups() {
        negativeLookups = true;
    }

    /* (non-Javadoc)
	 * @see com.palantir.atlasdb.table.description.TableDefinition#hasNegativeLookups()
	 */
    @Override
	public boolean hasNegativeLookups() {
        return negativeLookups;
    }

    public void maxValueSize(int size) {
        maxValueSize = size;
    }

    /* (non-Javadoc)
	 * @see com.palantir.atlasdb.table.description.TableDefinition#getMaxValueSize()
	 */
    @Override
	public int getMaxValueSize() {
        return maxValueSize;
    }

    public void genericTableName(String name) {
        genericTableName = name;
    }

    /* (non-Javadoc)
	 * @see com.palantir.atlasdb.table.description.TableDefinition#getGenericTableName()
	 */
    @Override
	public String getGenericTableName() {
        return genericTableName;
    }

    public void javaTableName(String name) {
        javaTableName = name;
    }

    /* (non-Javadoc)
	 * @see com.palantir.atlasdb.table.description.TableDefinition#getJavaTableName()
	 */
    @Override
	public String getJavaTableName() {
        return javaTableName;
    }

    private enum State {
        NONE,
        DEFINING_ROW_NAME,
        DEFINING_DYNAMIC_COLUMN,
        DEFINING_COLUMNS,
        DEFINING_CONSTRAINTS,
    }

    private State state = State.NONE;
    private int maxValueSize = Integer.MAX_VALUE;
    private String genericTableName = null;
    private String javaTableName = null;
    private List<NameComponentDescription> rowNameComponents = Lists.newArrayList();
    private List<NamedColumnDescription> fixedColumns = Lists.newArrayList();
    private List<NameComponentDescription> dynamicColumnNameComponents = Lists.newArrayList();
    private ColumnValueDescription dynamicColumnValue = null;
    private ConstraintMetadata.Builder constraintBuilder = ConstraintMetadata.builder();
    private boolean dbCompressionRequested = false;
    private boolean rangeScanAllowed = false;
    private boolean negativeLookups = false;
    private Set<String> fixedColumnShortNames = Sets.newHashSet();
    private Set<String> fixedColumnLongNames = Sets.newHashSet();
    private boolean noColumns = false;

    @Override
	public DefaultTableMetadata toTableMetadata() {
        Preconditions.checkState(!rowNameComponents.isEmpty(), "No row name components defined.");

        return new DefaultTableMetadata(
                new NameMetadataDescription(rowNameComponents),
                getColumnMetadataDescription(),
                conflictHandler,
                cachePriority,
                partitionStrategy,
                rangeScanAllowed,
                dbCompressionRequested,
                negativeLookups,
                sweepStrategy,
                expirationStrategy);
    }

    private ColumnMetadataDescription getColumnMetadataDescription() {
        if (!fixedColumns.isEmpty()) {
            Preconditions.checkState(
                    dynamicColumnNameComponents.isEmpty(),
                    "Cannot define both dynamic and fixed columns.");
            return new ColumnMetadataDescription(fixedColumns);
        } else {
            Preconditions.checkState(
                    !dynamicColumnNameComponents.isEmpty() && dynamicColumnValue != null,
                    "Columns not properly defined.");
            return new ColumnMetadataDescription(
                    new DynamicColumnDescription(new NameMetadataDescription(dynamicColumnNameComponents),
                    dynamicColumnValue));
        }
    }

	public ConstraintMetadata getConstraintMetadata() {
        return constraintBuilder.build();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private ColumnValueDescription getColumnValueDescription(Class protoOrPersistable, Compression compression) {
        if (GeneratedMessage.class.isAssignableFrom(protoOrPersistable)) {
            return ColumnValueDescription.forProtoMessage(protoOrPersistable, compression);
        } else if (Persister.class.isAssignableFrom(protoOrPersistable)) {
            return ColumnValueDescription.forPersister(protoOrPersistable, compression);
        } else if (Persistable.class.isAssignableFrom(protoOrPersistable)) {
            return ColumnValueDescription.forPersistable(protoOrPersistable, compression);
        } else {
            throw new IllegalArgumentException("Expected either protobuf or Persistable class.");
        }
    }
}
