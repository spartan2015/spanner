package com.excellenceengineeringsolutions.copydb;


import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import one.util.streamex.EntryStream;
import one.util.streamex.StreamEx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class CopyDbService {

    private static final List<Predicate<String>> EXCLUDE_TABLE_PREDICATES = Arrays.asList(
    );
    private static final Logger log = LoggerFactory.getLogger(CopyDbService.class);
    private final SpannerService spannerService;

    CopyDbService(SpannerService spannerService) {
        this.spannerService = spannerService;
    }

    private static boolean isCreateTableLine(String line) {
        return line.startsWith("CREATE TABLE");
    }

    void migrate(String fromInstance, String fromDatabase, String toInstance, String toDatabase) {
        log.info("Starting migration {}:{} -> {}:{}...", fromInstance, fromDatabase, toInstance, toDatabase);
        DatabaseClient databaseFrom = spannerService.getDatabaseClient(fromInstance, fromDatabase);
        DatabaseClient databaseTo = spannerService.getDatabaseClient(toInstance, toDatabase);

        List<String> ddl = spannerService.getDdl(toInstance, toDatabase);

        List<String> tablesToCopy = StreamEx.of(ddl)
                .filter(CopyDbService::isCreateTableLine)
                .filter(s -> EXCLUDE_TABLE_PREDICATES.stream().noneMatch(it -> it.test(s)))
                .toList();

        tablesToCopy.parallelStream()
                .forEach(table -> migrate(table, databaseFrom, databaseTo, toInstance, toDatabase));
    }

    private void migrate(String tableSchema, DatabaseClient databaseFrom, DatabaseClient databaseTo,
            String toInstanceId, String toDatabaseId) {
        List<String> lines = Splitter.on("\n").splitToList(tableSchema);
        String tableName = lines.get(0).replace("CREATE TABLE", "").replace("(", "").trim();
        log.info("Copying {}...", tableName);
        Map<String, String> columnDefinitions = StreamEx.of(lines)
                .skip(1) //Skip "CREATE TABLE ("
                .takeWhile(l -> l.startsWith(")")) // Read while ) - means end of column definitions
                .map(l -> l.trim().split(" "))
                .toMap(parts -> parts[0], parts -> parts[1]);

        log.info("Found column definitions {}",
                EntryStream.of(columnDefinitions).mapKeyValue((name, type) -> name + ": " + type)
                        .joining(System.lineSeparator() + "\t"));

        log.info("Truncating table {} in {}:{} ...", tableName, toInstanceId, toDatabaseId);
        databaseTo.singleUse().executeQuery(Statement.of("TRUNCATE TABLE " + tableName)).close();

        try (ResultSet rs = databaseFrom.singleUse().executeQuery(Statement.of("SELECT * FROM " + tableName))) {
            List<Mutation> mutations = new ArrayList<>();
            while (rs.next()) {
                Mutation.WriteBuilder builder = Mutation.newInsertBuilder(tableName);
                for (String column : columnDefinitions.keySet()) {
                    setToValue(builder, column, columnDefinitions.get(column), rs);
                }
                mutations.add(builder.build());
            }
            log.info("{} mutations to write", mutations.size());
            Lists.partition(mutations, 30).forEach(databaseTo::write);
        }
    }


    private void setToValue(Mutation.WriteBuilder builder, String column, String type, ResultSet rs) {
        if (!rs.isNull(column)) {
            if (type.startsWith("STRING")) {
                builder.set(column).to(rs.getString(column));
            } else if (type.equals("INT64")) {
                builder.set(column).to(rs.getLong(column));
            } else if (type.equals("TIMESTAMP")) {
                builder.set(column).to(rs.getTimestamp(column));
            } else if (type.startsWith("BYTES")) {
                builder.set(column).to(rs.getBytes(column));
            } else if (type.startsWith("BOOL")) {
                builder.set(column).to(rs.getBoolean(column));
            }
        }
    }
}
