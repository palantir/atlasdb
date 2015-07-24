package com.palantir.atlasdb.shell;

import java.util.List;

import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.table.description.TableMetadata;

/**
 * The callback ultimately called by the #view method in Ruby. This is an interface because behavior
 * must vary between the GUI and terminal-only version of the shell program.
 */
public interface AtlasShellGuiCallback {

    /**
     * Callback to display a graphical representation of the rows, optionally displaying only
     * specified columns matching on short or long columns names. A null or empty columns value will
     * display all columns.
     */
    void graphicalView(TableMetadata tableMetadata,
              String tableName,
              List<String> columns,
              List<RowResult<byte[]>> rows,
              boolean limitedResults);

    boolean isGraphicalViewEnabled();

    void clear();
}
