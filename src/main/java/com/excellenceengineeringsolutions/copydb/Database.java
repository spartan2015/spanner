package com.excellenceengineeringsolutions.copydb;

import com.google.cloud.spanner.DatabaseClient;
import one.util.streamex.StreamEx;

import java.util.List;

public class Database {
    public final DatabaseClient client;
    public final String instance;
    public final String name;
    public final List<Table> tables;

    public Database(DatabaseClient client, String instance, String name,
                    List<Table> tables) {
        this.client = client;
        this.instance = instance;
        this.name = name;
        this.tables = tables;
    }

    StreamEx<Table> tables() {
        return StreamEx.of(tables);
    }

    @Override
    public String toString() {
        return instance + ":" + name;
    }
}
