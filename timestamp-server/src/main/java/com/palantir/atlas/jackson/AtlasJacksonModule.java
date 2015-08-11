package com.palantir.atlas.jackson;

import javax.inject.Inject;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.palantir.atlas.api.TableCell;
import com.palantir.atlas.api.TableCellVal;
import com.palantir.atlas.api.TableRange;
import com.palantir.atlas.api.TableRowSelection;
import com.palantir.atlas.impl.TableMetadataCache;

public class AtlasJacksonModule {
    private final TableMetadataCache cache;

    @Inject
    public AtlasJacksonModule(TableMetadataCache cache) {
        this.cache = cache;
    }

    public Module createModule() {
        SimpleModule module = new SimpleModule("Atlas");
        module.addSerializer(new RangeTokenSerializer());
        module.addSerializer(new TableCellValSerializer(cache));
        module.addSerializer(new TableMetadataSerializer());
        module.addSerializer(new TableRangeSerializer());
        module.addSerializer(new TableRowResultSerializer(cache));
        module.addDeserializer(TableCellVal.class, new TableCellValDeserializer(cache));
        module.addDeserializer(TableRange.class, new TableRangeDeserializer(cache));
        module.addDeserializer(TableCell.class, new TableCellDeserializer(cache));
        module.addDeserializer(TableRowSelection.class, new TableRowSelectionDeserializer(cache));
        return module;
    }
}
