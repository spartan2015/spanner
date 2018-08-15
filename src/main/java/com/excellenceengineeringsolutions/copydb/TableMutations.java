package com.excellenceengineeringsolutions.copydb;

import java.util.List;

public class TableMutations {

    public final Table table;
    public final List<MutationWithSize> mutations;
    public final int size;

    public TableMutations(Table table, List<MutationWithSize> mutations, int size) {
        this.table = table;
        this.mutations = mutations;
        this.size = size;
    }
}
