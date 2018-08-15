package com.excellenceengineeringsolutions.copydb;

import com.google.cloud.spanner.Mutation;

public class MutationWithSize {

    public final Mutation mutation;
    public final int size;

    public MutationWithSize(Mutation mutation, int size) {
        this.mutation = mutation;
        this.size = size;
    }
}
