package com.excellenceengineeringsolutions.copydb;


import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.common.base.Stopwatch;
import one.util.streamex.EntryStream;
import one.util.streamex.StreamEx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class CopyDbService {

    public static final int SPANNER_MUTATIONS_PER_TRANSACTION_LIMIT = 20_000;
    private static final Logger log = LoggerFactory.getLogger(CopyDbService.class);
    private final MigrationValidation migrationValidation;
    private final DatabaseFactory databaseFactory;

    CopyDbService(MigrationValidation migrationValidation, DatabaseFactory databaseFactory) {
        this.migrationValidation = migrationValidation;
        this.databaseFactory = databaseFactory;
    }

    @Autowired
    private FromSpannerService fromSpannerService;
    @Autowired
    private ToSpannerService toSpannerService;

    void copy() {
        Database from = databaseFactory.load(fromSpannerService);
        Database to = toSpannerService.getSpannerProperties().isCreateNew() ?
                databaseFactory.newDatabase(fromSpannerService, from, toSpannerService) :
                databaseFactory.load(toSpannerService);
        migrationValidation.preValidation(from, to);
        Stopwatch migrationStopwatch = Stopwatch.createStarted();
        log.info("Starting migration {} -> {}...", from, to);

        Map<Boolean, List<TableMutations>> byHavingInterleavingTables = from.tables()
                .filter(table -> table.numberOfRows > 0) // Skip tables without data
                .parallel(new ForkJoinPool(200))
                .map(fromTable -> tableMutations(fromTable, from))
                .partitioningBy(tm -> tm.table.interleavedTables.isEmpty());

        List<MutationWithSize> independentMutations =
                StreamEx.of(byHavingInterleavingTables.get(true)).toFlatList(tm -> tm.mutations);
        List<MutationWithSize> mutationsWithInterleaving =
                StreamEx.of(byHavingInterleavingTables.get(false)).toFlatList(tm -> tm.mutations);
        int totalNumberOfMutations = independentMutations.size() + mutationsWithInterleaving.size();

        List<List<Mutation>> independentMutationChunks = splitIntoChunks(independentMutations);
        AtomicLong mutationCounter = new AtomicLong();
        EntryStream.of(independentMutationChunks)
                .parallel(new ForkJoinPool(200))
                .forKeyValue((index, mutationChunk) -> {
                    Timestamp timestamp = to.client.write(mutationChunk);
                    long progress = mutationCounter.addAndGet(mutationChunk.size());
                    log.info("{} / {} mutations at {}", progress, totalNumberOfMutations, timestamp);
                });

        List<List<Mutation>> dependentMutationChunks = splitIntoChunks(mutationsWithInterleaving);
        EntryStream.of(dependentMutationChunks)
                .forKeyValue((index, mutationChunk) -> {
                    Timestamp timestamp = to.client.write(mutationChunk);
                    long progress = mutationCounter.addAndGet(mutationChunk.size());
                    log.info("{} / {} mutations at {}", progress, totalNumberOfMutations, timestamp);
                });

        migrationValidation.postValidation(from, to);
        log.info("Migrated in {} seconds", migrationStopwatch.elapsed(TimeUnit.SECONDS));
    }

    private List<List<Mutation>> splitIntoChunks(List<MutationWithSize> allMutations) {
        List<List<Mutation>> mutationChunks = new ArrayList<>();
        int currentList = 0;
        int sizeLeft = SPANNER_MUTATIONS_PER_TRANSACTION_LIMIT;
        mutationChunks.add(new ArrayList<>());
        for (MutationWithSize mutation : allMutations) {
            int mutationSize = mutation.size;
            if (sizeLeft - mutationSize < 0) {
                currentList++;
                sizeLeft = SPANNER_MUTATIONS_PER_TRANSACTION_LIMIT;
                mutationChunks.add(new ArrayList<>());
            }
            sizeLeft -= mutationSize;
            mutationChunks.get(currentList).add(mutation.mutation);
        }
        return mutationChunks;
    }

    private TableMutations tableMutations(Table table, Database from) {
        List<MutationWithSize> mutations = new ArrayList<>(getMutationsOfTable(table, from));
        for (Table interleavedTable : table.interleavedTables) {
            mutations.addAll(getMutationsOfTable(interleavedTable, from));
        }
        return new TableMutations(table, mutations, mutations.stream().mapToInt(m -> m.size).sum());
    }

    private List<MutationWithSize> getMutationsOfTable(Table table, Database from) {
        try (ResultSet rs = from.client.singleUse()
                .executeQuery(Statement.of("SELECT * FROM " + table.name))) {

            List<MutationWithSize> mutations = new ArrayList<>();
            while (rs.next()) {
                Mutation.WriteBuilder builder = Mutation.newInsertBuilder(table.name);
                for (String column : table.columnsToTypes.keySet()) {
                    setToValue(builder, column, table.columnsToTypes.get(column), rs);
                }
                Mutation mutation = builder.build();
                int size = table.columnsToTypes.size() + table.numberOfIndexes;
                mutations.add(new MutationWithSize(mutation, size));
            }
            log.info("{} with {} write mutations", table.name, mutations.size());
            return mutations;
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
