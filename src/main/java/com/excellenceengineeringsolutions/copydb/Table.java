package com.excellenceengineeringsolutions.copydb;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Table {

    public final String schema;
    public final String name;
    public final long numberOfRows;
    public final int numberOfIndexes;
    public final Map<String, String> columnsToTypes;
    public final List<Table> interleavedTables;

    public Table(String schema, String name, long numberOfRows, int numberOfIndexes, Map<String, String> columnsToTypes) {
        this(schema, name, numberOfRows, numberOfIndexes, columnsToTypes, Collections.emptyList());
    }

    public Table(String schema, String name, long numberOfRows, int numberOfIndexes, Map<String, String> columnsToTypes, List<Table> interleavedTables) {
        this.schema = schema;
        this.name = name;
        this.numberOfRows = numberOfRows;
        this.numberOfIndexes = numberOfIndexes;
        this.columnsToTypes = columnsToTypes;
        this.interleavedTables = interleavedTables;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Table table = (Table) o;

        if (schema != null ? !schema.equals(table.schema) : table.schema != null) {
            return false;
        }
        return interleavedTables != null ? interleavedTables.equals(table.interleavedTables)
                : table.interleavedTables == null;
    }

    @Override
    public int hashCode() {
        int result = schema != null ? schema.hashCode() : 0;
        result = 31 * result + (interleavedTables != null ? interleavedTables.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Table{" +
                "name='" + name + '\'' +
                ", numberOfIndexes=" + numberOfIndexes +
                ", columnsToTypes=" + columnsToTypes +
                ", interleavedTables=" + interleavedTables +
                '}';
    }
}
