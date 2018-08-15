package com.excellenceengineeringsolutions.copydb;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

@Component
public class MigrationValidation {

    private static final Logger log = LoggerFactory.getLogger(MigrationValidation.class);

    public void preValidation(Database from, Database to) {
        log.info("Asserting that migration {} -> {} is possible...", from, to);
        assertThatDDLsAreSame(from, to);
        assertThatToIsEmpty(to);
    }

    public void postValidation(Database from, Database to) {
        verifyDatabasesHaveSameRows(from, to);
    }

    private void verifyDatabasesHaveSameRows(Database from, Database to) {
        log.info("Verifying database have same number of rows...");
        Map<Table, Long> fromTablesToNumberOfRows = from
                .tables()
                .parallel(new ForkJoinPool(200))
                .toMap(Function.identity(), table -> {
            try (ResultSet rs = from.client.singleUse().executeQuery(Statement.of("SELECT COUNT(*) FROM " + table.name))) {
                return rs.next() ? rs.getLong(0) : 0;
            }
        });
        Map<Table, Long> toTablesToNumberOfRows = to
                .tables()
                .parallel(new ForkJoinPool(200))
                .toMap(Function.identity(), table -> {
            try (ResultSet rs = to.client.singleUse().executeQuery(Statement.of("SELECT COUNT(*) FROM " + table.name))) {
                return rs.next() ? rs.getLong(0) : 0;
            }
        });
        fromTablesToNumberOfRows.forEach((fromTable, fromTableNumberOfRows) -> {
            Long toTableNumberOfRows = toTablesToNumberOfRows.get(fromTable);
            if (!fromTableNumberOfRows.equals(toTableNumberOfRows)) {
                throw new MigrationFailed(String.format("Table %s have different number of rows in %s:%d and %s:%d",
                        fromTable.name, from, fromTableNumberOfRows, to, toTableNumberOfRows));
            }
        });
    }

    private void assertThatToIsEmpty(Database to) {
        log.info("Asserting that {} is empty ...", to);
        to.tables.forEach(table -> {
            if (table.numberOfRows != 0) {
                throw new MigrationImpossible(String.format("Table %s in %s contains data. "
                        + "Currently it is not supported", table, to));
            }
        });
    }

    private void assertThatDDLsAreSame(Database from, Database to) {
        log.info("Asserting that {} and {} have identical tables...", from, to);
        if (!from.tables.equals(to.tables)) {
            throw new MigrationImpossible(String.format("Tables of %s and %s are different. "
                            + "Currently it is not supported. To tables: %s %s From tables: %s",
                    from, to, to.tables, System.lineSeparator(), from.tables));
        }
    }
}
