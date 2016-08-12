package com.palantir.atlasdb.sql;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.palantir.common.annotation.Modified;


public final class AdjustableTextTable {
    private static final String LEFT_SPACE = "  ";
    private static final String RIGHT_SPACE = "  ";
    private static final String DIVIDER = "|";
    private static final String LINE_DIVIDER = "+";
    private static final int SPACER_WIDTH = LEFT_SPACE.length() + RIGHT_SPACE.length() + DIVIDER.length();

    private int sum;
    private List<Row> rows = Lists.newArrayList();

    private int[] width;  // must have nColumns elements
    private int nColumns;

    private String name;
    private int[] numToMerge = null;        // state: how many columns to merge on the next row addition
    private String[] formatColumns = null;  // state: how to format upcoming columns
    private Supplier<String> defaultString;
    private Supplier<String> hline;

    /**
     *
     *              BUILDER METHODS
     *
     */
    public AdjustableTextTable setName(String name) {
        this.name = name;
        return this;
    }

    public AdjustableTextTable addRow(Object... values) {
        Preconditions.checkNotNull(values, "Array of provided values may not be null.");
        if (numToMerge != null) {
            rows.add(new RowWithMergedColumns(values, formatColumns, numToMerge));
            int nCells = 0;
            for (int i : numToMerge) {
                nCells += i;
            }
            nColumns = Math.max(nColumns, nCells);
        } else {
            rows.add(new DefaultRow(values, formatColumns));
        }
        nColumns = Math.max(values.length, nColumns);
        resetState();
        return this;
    }

    AdjustableTextTable addRows(Object[][] values) {
        if (values != null && values.length != 0) {
            if (numToMerge != null) {
                for (Object[] value : values) {
                    Preconditions.checkNotNull(value, "Array of provided values may not be null.");
                    rows.add(new RowWithMergedColumns(value, formatColumns, numToMerge));
                    nColumns = Math.max(value.length, nColumns);
                }
                int nCells = 0;
                for (int i : numToMerge) {
                    nCells += i;
                }
                nColumns = Math.max(nColumns, nCells);
            } else {
                for (Object[] value : values) {
                    rows.add(new DefaultRow(value, formatColumns));
                    nColumns = Math.max(value.length, nColumns);
                }
            }
        }
        resetState();
        return this;
    }

    public AdjustableTextTable addHline() {
        rows.add(new Hline());
        return this;
    }

    /** Prepares next {@link #addRow(Object...)} or {@link #addRows(Object[][])} to merge columns according to the specification
     * @param numToMerge specification of how many columns to merge
     * */
    public AdjustableTextTable mergeColumns(int... numToMerge) {
        Preconditions.checkNotNull(numToMerge, "Merge column spec cannot be null");
        this.numToMerge = numToMerge;
        return this;
    }

    /** Prepares next {@link #addRow(Object...)} or {@link #addRows(Object[][])} to merge columns according to the specification
     * @param formatColumns array of standard Java formatting strings, e.g. %5.2f
     * */
    AdjustableTextTable formatColumns(String... formatColumns) {
        this.formatColumns = formatColumns;
        return this;
    }

    @Override
    public String toString() {
        width = standardizeColumns();
        StringBuilder sb = new StringBuilder();
        sb.append(name != null && name.length()>0 ? name + "\n" : "");
        for (Row row : rows) {
            sb.append(row.render(this));
        }
        return sb.toString();
    }

    /*
                    PRIVATE METHODS
     */

    private void resetState() {
        this.numToMerge = null;
        this.formatColumns = null;
    }

    private String buildHorizontalLine() {
        StringBuilder sb = new StringBuilder(repeatSymbol('-', Math.max(sum + SPACER_WIDTH * width.length - LINE_DIVIDER.length(), 0)));
        int runningLength = 0;
        for (int i = 0; i < width.length - 1; ++i) {   // dividers are only between columns
            runningLength += SPACER_WIDTH + width[i];
            sb.replace(runningLength - LINE_DIVIDER.length(), runningLength, LINE_DIVIDER);
        }
        return sb.append("\n").toString();
    }

    private static @Nonnull
    String[] objectsToStrings(@Nonnull Object[] values, @Nullable String[] formats) {
        String[] result = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            Object val = values[i];
            if (formats != null && formats.length > i) {
                result[i] = String.format(formats[i], val);
            } else if (val instanceof Double || val instanceof Float) { /* the rest are defaults */
                result[i] = String.format("%.2f", val);
            } else if (val instanceof Long || val instanceof Integer) {
                result[i] = String.format("%d", val);
            } else {
                result[i] = String.format("%s", val);
            }
        }
        return result;
    }

    private @Nonnull
    int[] standardizeColumns() {
        int[] width = new int[nColumns];
        for (Row row : rows) {
            width = row.updateWidth(width, nColumns);
        }
        sum = 0;
        for (int i : width) {
            sum += i;
        }
        hline = Suppliers.memoize(new Supplier<String>() {
            @Override
            public String get() {
                return buildHorizontalLine();
            }
        });
        defaultString = Suppliers.memoize(new Supplier<String>() {
            @Override
            public String get() {
                return buildDefaultLine();
            }
        });
        return width;
    }

    private String buildDefaultLine() {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        // j is the next column in skipColumns that we are going through
        // leftToMerge is the decreasing count of columns
        // columnWidth is the increasing width of column
        for (; i < width.length; i++) {
            sb.append(LEFT_SPACE)
                    .append("%")
                    .append(i == 0 ? "-" : "").append(width[i]).append("s")
                    .append(RIGHT_SPACE);
            if (i < width.length - 1) {
                sb.append(DIVIDER);
            } else {
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    private String buildFormattingLine(String[] valuesRow, int[] mergeRow) {
        StringBuilder sb = new StringBuilder();
        int i = 0, j = 0, columnWidth = 0;
        int leftToMerge = (mergeRow.length > j) ? mergeRow[j] : 1;
        String formatCell;
        boolean toCenter = leftToMerge > 1;
        // j is the next column in skipColumns that we are going through
        // leftToMerge is the decreasing count of columns
        // columnWidth is the increasing width of column
        for (; i < width.length; i++) {
            if (leftToMerge == 1) {   // add spacer, close the column
                columnWidth += width[i];
                if (toCenter && j < valuesRow.length) {
                    int leftGap = (int) Math.floor((columnWidth - valuesRow[j].length()) / 2.0);
                    int rightGap = (int) Math.ceil((columnWidth - valuesRow[j].length()) / 2.0);
                    assert (leftGap + valuesRow[j].length() + rightGap == columnWidth);
                    formatCell = repeatSymbol(' ', leftGap) + "%s" + repeatSymbol(' ', rightGap);
                } else {
                    formatCell = "%" + columnWidth + "s";
                }
                sb.append(LEFT_SPACE)
                        .append(formatCell)
                        .append(RIGHT_SPACE);
                if (i < width.length - 1) {
                    sb.append(DIVIDER);
                } else {
                    sb.append("\n");
                }
                columnWidth = 0;
                j++;
                leftToMerge = (mergeRow.length > j) ? mergeRow[j] : 1;
                toCenter = leftToMerge > 1;
            } else {  // merge with the next column
                columnWidth += SPACER_WIDTH + width[i];
                leftToMerge--;
            }
            assert (leftToMerge >= 1);
        }
        return sb.toString();
    }

    private static String repeatSymbol(char symbol, int newWidth) {
        char[] dashes = new char[newWidth];
        Arrays.fill(dashes, symbol);
        return new String(dashes);
    }

    /**
     *                  ROWS
     */
    private interface Row {
        String render(AdjustableTextTable table);
        int[] updateWidth(final int[] width, int nColumns);
    }



    private static class Hline implements Row {

        @Override
        public String render(AdjustableTextTable table) {
            return table.hline.get();
        }

        @Override
        public int[] updateWidth(final int[] width, final int nColumns) {
            return width;
        }
    }



    private static class DefaultRow implements Row {
        @Nonnull public String[] cells;

        DefaultRow(@Nonnull final Object[] values, final String[] formatColumns) {
            this.cells = objectsToStrings(values, formatColumns);
        }

        @Override
        public String render(AdjustableTextTable table) {
            return String.format(table.defaultString.get(), (Object[]) cells);
        }

        @Override
        public @Nonnull int[] updateWidth(@Nonnull @Modified final int[] width, final int nColumns) {
            for (int k = 0; k < nColumns; ++k) {
                width[k] = Math.max(0, width[k]);
                if (k < cells.length && cells[k] != null) {
                    width[k] = Math.max(width[k], cells[k].length());
                }
            }
            return width;
        }
    }



    private static class RowWithMergedColumns extends DefaultRow {
        @Nonnull int[] numToMerge;

        RowWithMergedColumns(@Nonnull final Object[] values, final String[] formatColumns, final int[] numToMerge) {
            super(values, formatColumns);
            if (values.length != numToMerge.length) {
                throw new IllegalArgumentException(
                        "Number of added values must correspond to the column instructions.\n" +
                                "Current merging: " + Arrays.toString(numToMerge) + "\n" +
                                "Values to add: " + Arrays.toString(values));
            }
            this.numToMerge = numToMerge;
        }

        @Override
        public String render(AdjustableTextTable table) {
            final String formattingLine = table.buildFormattingLine(cells, numToMerge);
            return String.format(formattingLine, (Object[]) cells);
        }

        @Override
        public @Nonnull int[] updateWidth(@Nonnull @Modified final int[] width, final int nColumns) {
            int[] merge = numToMerge;
            int runLength = 0;
            // go through every merge and split the width evenly
            for (int i = 0; i < merge.length; ++i) {
                int numToMerge = merge[i];
                int evenWidth = (int) Math.ceil((float) (cells[i].length() + SPACER_WIDTH) / numToMerge) - SPACER_WIDTH;
                // round up
                assert (runLength + numToMerge <= nColumns);
                for (int k = runLength; k < runLength + numToMerge; ++k) {
                    width[k] = Math.max(width[k], evenWidth);
                }
                runLength += numToMerge;
            }
            return width;
        }
    }

}
