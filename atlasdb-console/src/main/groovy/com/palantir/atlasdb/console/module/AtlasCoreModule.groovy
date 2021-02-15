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
package com.palantir.atlasdb.console.module

import com.codahale.metrics.MetricRegistry
import com.fasterxml.jackson.databind.ObjectMapper
import com.palantir.atlasdb.api.AtlasDbService
import com.palantir.atlasdb.api.TransactionToken
import com.palantir.atlasdb.config.AtlasDbConfig
import com.palantir.atlasdb.config.AtlasDbConfigs
import com.palantir.atlasdb.console.AtlasConsoleModule
import com.palantir.atlasdb.console.AtlasConsoleService
import com.palantir.atlasdb.console.AtlasConsoleServiceImpl
import com.palantir.atlasdb.console.AtlasConsoleServiceWrapper
import com.palantir.atlasdb.console.exceptions.InvalidTableException
import com.palantir.atlasdb.factory.TransactionManagers
import com.palantir.atlasdb.impl.AtlasDbServiceImpl
import com.palantir.atlasdb.impl.TableMetadataCache
import com.palantir.atlasdb.jackson.AtlasJacksonModule
import com.palantir.atlasdb.transaction.api.TransactionManager
import com.palantir.conjure.java.api.config.service.UserAgent
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry
import groovy.json.JsonBuilder
import groovy.json.JsonOutput
import groovy.transform.CompileStatic

/**
 * Public methods that clients can call within AtlasConsole.
 * Additional public methods contained in Table.groovy and Range.groovy
 */
@CompileStatic
class AtlasCoreModule implements AtlasConsoleModule {
    private static final Map helpWithoutMutations = [
            'tables': '''\
                         Use tables() to view a list of all tables in AtlasDB accessible
                         by AtlasConsole. Does not include legacy tables. Ex: tables()

                         Use table(<TABLE-NAME>) to get a reference to a table in AtlasDB.
                         Ex: def x = table('stream_value')

                         To retrieve metadata about a table, use <TABLE>.describe(),
                         <TABLE>.isDynamic(), <TABLE>.rowComponents(), and <TABLE>.columnNames().
                         '''.stripIndent(),

            'transactions': '''\
                         AtlasDB implements fully ACID transactions with snapshot isolation. By
                         default, a transaction will be created for every statement you execute.
                         To group several statements in a single transaction, you can use the
                         following commands:

                         - startTransaction() to start a transaction.
                         - currentTransaction() to view all operations in the current transaction.
                         - endTransaction() to end and commit the current transaction.
                         - abortTransaction() to end and abort (undo) the current transaction.
                         '''.stripIndent(),

            'prettyPrint': '''\
                         Print tables and objects in a more human-readable format.

                         Use pp() to print objects. Ex: pp(obj).

                         Use pptable() to print ranges or lists of rows from a table. Ex:
                         pptable(table(<TABLE-NAME>)).getRange(), ['row': [width: 30],
                         'base_object': [width: 50, format: 'json']])
                         '''.stripIndent(),

            'getRange': '''\
                         Retrieve a range of rows from a table.

                         Use getRange() to get all rows from the table

                         Use getRange(args) to get a subset. Ex:

                         <TABLE>.getRange([start: <START-ROW-VALUE>, end: <END-ROW-VALUE>
                         , prefix: <OBJECT-ID-OR-ROW-VALUE>, cols: [<COL-NAMES>]])
                         '''.stripIndent(),

            'getRow': '''\
                         Retrieve a single row from a table. Ex:

                         <TABLE>.getRows(<ROW-VALUE>).

                         <ROW-VALUE> here can be either a list of row component values (e.g. [0,1,2])
                         or a map including the row component names (e.g. [obj_id:0, foo_id:1, bar_id:2])
                         '''.stripIndent(),

            'getRows': '''\
                         Retrieve one or more rows from a table. Ex:

                         <TABLE>.getRows([ <ROW-VALUE>, <ROW-VALUE> ])

                         <ROW-VALUE> here can be either a list of row component values (e.g. [0,1,2])
                         or a map including the row component names (e.g. [obj_id:0, foo_id:1, bar_id:2])
                         '''.stripIndent(),

            'join': '''\
                         Retrieve one or more rows from a table and match them up by row-key
                         with corresponding values.  Ex:

                         <TABLE>.join([[ <ROW-VALUE-1> : <EXTRA-DATA-1>], [ <ROW-VALUE-2> : <EXTRA-DATA-2]])
                         Returns:
                         [["JOIN_KEY" : <ROW-VALUE-1>, "INPUT_VALUE" : <EXTRA-DATA-1>, "OUTPUT_VALUE" : <TABLE-ROW-1>], 
                         ["JOIN_KEY" : <ROW-VALUE-2>, "INPUT_VALUE" : <EXTRA-DATA-2>, "OUTPUT_VALUE" : <TABLE-ROW-2>]]

                         The join() method partitions the input iterable into batches, default 10000.
                         Use join(iterable, cols, batchSize) to override.

                         For an easy way to lazily transform a table range into the appropriate format for joining,
                         consider Guava (since Groovy's Iterable#collect is not lazy).  For example, if two tables have
                         the same structure in their row keys, a full join looks like this:

                         FluentIterable = com.google.common.collect.FluentIterable
                         input = FluentIterable.from(table("myTable").getRange()).transform{ [(it.row): it}
                         output = table("myOtherTable").join(input)
                         output.each { println it}
                         '''.stripIndent(),


            'getCells': '''\
                         Retrieve one or more cells from a table, specified by row and
                         column.

                         Ex (for named columns): <TABLE>.getCells([row: <ROW-VALUE>,
                         col: <COL-NAME>])

                         Ex (for dynamic columns): <TABLE>.getCells([row: <ROW-VALUE>,
                         col: <COL-INDEX>])

                         <ROW-VALUE> here can be either a list of row component values (e.g. [0,1,2])
                         or a map including the row component names (e.g. [obj_id:0, foo_id:1, bar_id:2])
                         '''.stripIndent(),

            'connect': '''\
                          Connect to an atlas server using settings from a dropwizard yaml
                          file. Ex:

                          connect(<PATH-TO-YAML-CONFIG>)
                          '''.stripIndent(),
    ]
    private static final Map mutationsHelp = [
            'put': '''\
                         Insert or update one or more rows in a table.

                         Ex (for named columns): <TABLE>.put([row: <ROW-VALUE>, cols:
                         [<COL-NAME>: <COL-VALUE>]])

                         Ex (for dynamic columns): <TABLE>.put([row: <ROW-VALUE>, col:
                         <COL-INDEX>, val: <COL-VALUE>])
                         '''.stripIndent(),

            'delete': '''\
                         Delete one or more rows in a table. Ex:

                         <TABLE>.delete([row: <ROW-VALUE>, cols: [<COL-NAME>, <COL-NAME>]])
                          '''.stripIndent(),

    ]
    private static final int COLUMN_PADDING = 5

    private AtlasConsoleServiceWrapper atlasConsoleServiceWrapper

    private Map bindings
    private static boolean mutationsEnabled

    public AtlasCoreModule(AtlasConsoleServiceWrapper atlasConsoleServiceWrapper, boolean mutationsEnabled) {
        this.mutationsEnabled = mutationsEnabled
        this.atlasConsoleServiceWrapper = atlasConsoleServiceWrapper
        this.bindings = [
                'table': this.&table,
                'tables': this.&tables,
                'startTransaction': this.&startTransaction,
                'endTransaction': this.&endTransaction,
                'currentTransaction': this.&currentTransaction,
                'abortTransaction': this.&abortTransaction,
                'pp': this.&pp,
                'pptable': this.&pptable,
                'connect': this.&connect,
                'connectInline': this.&connectInline
        ]
    }

    @Override
    Map<String, String> getHelp() {
        if (!mutationsEnabled) {
            return helpWithoutMutations
        } else {
            return helpWithoutMutations + mutationsHelp
        }
        return helpWithoutMutations
    }

    @Override
    Map<String, Closure> getBindings() {
        return bindings
    }

    public connect(String yamlFilePath) {
        setupConnection(AtlasDbConfigs.load(new File(yamlFilePath), AtlasDbConfig.class))
    }

    public connectInline(String fileContents) {
        setupConnection(AtlasDbConfigs.loadFromString(fileContents, null, AtlasDbConfig.class))
    }

    private setupConnection(AtlasDbConfig config) {
        TransactionManager tm = TransactionManagers.builder()
                .config(config)
                .userAgent(UserAgent.of(UserAgent.Agent.of("atlasdb-console", "0.0.0")))
                .globalMetricsRegistry(new MetricRegistry())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .allowHiddenTableAccess(true)
                .build()
                .serializable()
        TableMetadataCache cache = new TableMetadataCache(tm.getKeyValueService())
        AtlasDbService service = new AtlasDbServiceImpl(tm.getKeyValueService(), tm, cache)
        ObjectMapper serviceMapper = new ObjectMapper()
        serviceMapper.registerModule(new AtlasJacksonModule(cache).createModule())

        AtlasConsoleService atlasConsoleService = new AtlasConsoleServiceImpl(service, serviceMapper)
        atlasConsoleServiceWrapper = AtlasConsoleServiceWrapper.init(atlasConsoleService)
    }

    public tables() {
        def tableList = atlasConsoleServiceWrapper.tables()
        pp(tableList)
        return tableList
    }

    public Table table(String name) {
        if (!atlasConsoleServiceWrapper.tables().contains(name)) {
            throw new InvalidTableException("Table '" + name + "' does not exist")
        }
        new Table(name, atlasConsoleServiceWrapper, mutationsEnabled)
    }

    public startTransaction() {
        atlasConsoleServiceWrapper.startTransaction()
    }

    public endTransaction(TransactionToken token = atlasConsoleServiceWrapper.getTransactionToken()) {
        atlasConsoleServiceWrapper.endTransaction(token)
    }

    public abortTransaction(TransactionToken token = atlasConsoleServiceWrapper.getTransactionToken()) {
        atlasConsoleServiceWrapper.abort(token)
    }

    public currentTransaction() {
        pp(atlasConsoleServiceWrapper.currentTransaction())
    }

    public static void pp(input) {
        def obj
        switch(input) {
            case null:
                println "null"
                return
            case Range:
                obj = input.iterator()
                break
            case Table:
                obj = ((Table)input).getDescription()
                break
            default:
                obj = input
                break
        }
        JsonBuilder builder = new JsonBuilder()
        builder(obj)
        println(builder.toPrettyString())
    }

    /**
     * Print a table or subset of a table in tabular format.
     *
     * @param range     Map of rows to print. Ex: table('ds').getRange()
     *
     * @param cols      Map of columns to print. Column names map to a Map of optional
     *                  parameters for the column:
     *                      width:      Width in number of characters.
     *                      format:     'json' to print in JSON pretty-print format, or 'string'
     *                                  (default) to print as a simple string.
     *
     * @param options   Map of optional parameters for the table:
     *                      maxLines:   Maximum number of lines to print per cell. Defaults to
     *                                  100 if unspecified.
     *                      closure:    Groovy closure (print row only if closure returns true).
     *                                  Defaults to { true }.
     *
     *                  Example query:
     *                      pptable(    table('ds').getRange(),
     *                                  ['datasource': ['width': 60, 'format': 'json']],
     *                                  ['maxLines': 20])
     *
     * @return No return value; print only.
     */
    def pptable(range, Map <String, Map> cols = [:], Map options = [:]) {
        cols.each { key, val ->
            if (!val['width']) {
                val['width'] = key.length() + COLUMN_PADDING
            }
            if (!val['format']) {
                val['format'] = 'string'
            }
        }
        if (!options['maxLines']) {
            options['maxLines'] = 100
        }
        if (!options['closure']) {
            options['closure'] = { true }
        }
        printHeader(cols)
        range.each {
            if ((options['closure'] as Closure).call(it)) {
                printRow(it, cols, options['maxLines'] as int)
            }
        }
    }

    /* Prints column names and two lines separating the header from the table contents.
     */
    private printHeader(Map<String, Map> cols) {
        cols.each { key, val ->
            print key.padRight(val['width'] as int)
        }
        int tableWidth = cols.inject(0) { sum, cur ->
            return sum + (cur.value['width'] as int)
        } as int
        print '\n' + ('=' * tableWidth) + '\n'
    }

    /* Returns a List of Strings, each <width> characters long, of maximum size
     * <maxLines>. The list is obtained by splitting the String <val> on every '\n'
     * character or every <width> characters if those characters do not contain a '\n'.
     */
    private List<String> splitCellString(String val, int width, int maxLines) {
        if (val.length() > (maxLines * (width - COLUMN_PADDING))) {
            val = val.substring(0, (maxLines * (width - COLUMN_PADDING)))
        }

        // Regular expression for inserting a '\n' every <width> characters
        val = val.replaceAll('(.{' + (width - COLUMN_PADDING) + '})', '$1\n')

        if (val == null) {
            return [' ' * width]
        } else {
            List<String> valToArray = val.tokenize('\n')
            return valToArray.collect {
                it.padRight(width)
            }
        }
    }

    /* Returns a nested component from within a row. This can be a cell, a member of
     * a cell, a member of a member of a cell, etc. The component is obtained by
     * splitting <name> on every '.' character and evaluating each level of nesting
     * as either a List or a Map.
     */
    private getColumnComponent(row, String name) {
        def colComponents = name.tokenize('.')
        def col = row
        for (component in colComponents) {
            if (component.isNumber()) {
                def index = component.toInteger()
                col = (col as List)[index]
            } else {
                col = (col as Map)[component]
            }
            if (col == null) {
                break
            }
        }
        return col
    }

    /* Prints the contents of <row>, filtered by the components specified in <cols>.
     * The cells are printed in tabular format with each horizontal row representing
     * a row in the table and each vertical column representing a column or column
     * member specified by the user.
     */

    private printRow(row, Map <String, Map> cols, int maxLines) {
        List<List<String>> colValues = []
        cols.each { key, val ->
            def component = getColumnComponent(row, key)
            if (val['format'] == 'json') {
                component = JsonOutput.prettyPrint(JsonOutput.toJson(component))
            }
            colValues.add(splitCellString(component.toString(),
                                        val['width'] as int,
                                        maxLines))
        }

        int maxArraySize = (colValues.max { it.size() }).size()
        int maxLinesToPrint = (maxLines < maxArraySize) ? maxLines : maxArraySize

        for (int i = 0; i < maxLinesToPrint; i++) {
            colValues.each {
                if (it.size() >= i && it[i]) {
                    print it[i]
                } else {
                    print (' ' * it[0].length())
                }
            }
            print '\n'
        }
        print '\n'
    }

}
