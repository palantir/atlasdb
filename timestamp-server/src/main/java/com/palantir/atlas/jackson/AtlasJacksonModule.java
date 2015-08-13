package com.palantir.atlas.jackson;

import javax.inject.Inject;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.palantir.atlas.api.RangeToken;
import com.palantir.atlas.api.TableCell;
import com.palantir.atlas.api.TableCellVal;
import com.palantir.atlas.api.TableRange;
import com.palantir.atlas.api.TableRowResult;
import com.palantir.atlas.api.TableRowSelection;
import com.palantir.atlas.impl.TableMetadataCache;
import com.palantir.atlasdb.table.description.TableMetadata;

public class AtlasJacksonModule {
    private final TableMetadataCache cache;

    @Inject
    public AtlasJacksonModule(TableMetadataCache cache) {
        this.cache = cache;
    }

    public Module createModule() {
        SimpleModule module = new SimpleModule("Atlas");
        module.addSerializer(new RangeTokenSerializer());
        module.addSerializer(new TableCellSerializer(cache));
        module.addSerializer(new TableCellValSerializer(cache));
        module.addSerializer(new TableMetadataSerializer());
        module.addSerializer(new TableRangeSerializer());
        module.addSerializer(new TableRowResultSerializer(cache));
        module.addSerializer(new TableRowSelectionSerializer(cache));
        module.addDeserializer(RangeToken.class, new RangeTokenDeserializer());
        module.addDeserializer(TableCell.class, new TableCellDeserializer(cache));
        module.addDeserializer(TableCellVal.class, new TableCellValDeserializer(cache));
        module.addDeserializer(TableMetadata.class, new TableMetadataDeserializer());
        module.addDeserializer(TableRange.class, new TableRangeDeserializer(cache));
        module.addDeserializer(TableRowResult.class, new TableRowResultDeserializer(cache));
        module.addDeserializer(TableRowSelection.class, new TableRowSelectionDeserializer(cache));
        return module;
    }
}
