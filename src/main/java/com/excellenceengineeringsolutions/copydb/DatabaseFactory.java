package com.excellenceengineeringsolutions.copydb;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.common.base.Splitter;
import com.google.common.collect.ArrayListMultimap;
import one.util.streamex.StreamEx;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;

@Service
public class DatabaseFactory
{

  private static final Logger log = LoggerFactory.getLogger(DatabaseFactory.class);

  private final List<Predicate<String>> excludeTablePredicates;

  DatabaseFactory(ToDbProperties properties)
  {
    excludeTablePredicates = StreamEx.of(Arrays.asList(properties
      .getIgnore()))
      .map(tableName -> (Predicate<String>) (s) -> s.startsWith(tableName))
      .toList();
  }

  private static boolean isCreateTableLine(String line)
  {
    return line.startsWith("CREATE TABLE");
  }

  private static boolean isCreateIndexLine(String line)
  {
    return line.startsWith("CREATE UNIQUE INDEX") || line.startsWith("CREATE INDEX");
  }

  private static boolean isChildTable(String tableSchema)
  {
    return tableSchema.contains("INTERLEAVE IN PARENT ");
  }

  private static String schemaToTableName(String schema)
  {
    List<String> lines = Splitter.on("\n")
      .splitToList(schema);
    return lines.get(0)
      .replace("CREATE TABLE", "")
      .replace("(", "")
      .trim();
  }

  public Database load(SpannerService spannerService)
  {
    log.info("Loading {}:{}...", spannerService.getSpannerProperties()
      .getInstance(), spannerService.getSpannerProperties()
      .getDatabase());
    final DatabaseClient dbClient = spannerService.getDatabaseClient();
    final List<String> ddl = spannerService.getDdl();

    List<String> tableSchemas = StreamEx.of(ddl)
      .filter(DatabaseFactory::isCreateTableLine)
      .toList();

    Map<String, LongAdder> tableNamesToNumberOfIndexes = buildTablesToIndexesMap(ddl);
    ArrayListMultimap<String, Table> tablesWithInterLeaving = buildTablesToTablesMap(dbClient,
      tableNamesToNumberOfIndexes, ddl);

    List<Table> tables = StreamEx.of(tableSchemas)
      .parallel()
      .filter(schema -> !isChildTable(schema))
      .map(schema ->
      {
        Optional<Table> table = schemaToTable(dbClient, tableNamesToNumberOfIndexes, schema);
        return table.map(t ->
        {
          if ( tablesWithInterLeaving.containsKey(t.name) )
          {
            return new Table(t.schema,
              t.name, t.numberOfRows, t.numberOfIndexes, t.columnsToTypes,
              tablesWithInterLeaving.get(t.name));
          }
          return t;
        });
      })
      .filter(Optional::isPresent)
      .map(Optional::get)
      .toList();
    return new Database(dbClient, spannerService.getSpannerProperties()
      .getInstance(), spannerService.getSpannerProperties()
      .getDatabase(), tables);
  }

  private Optional<Table> schemaToTable(DatabaseClient dbClient, Map<String, LongAdder> tableNamesToNumberOfIndexes,
                                        String schema)
  {
    String tableName = schemaToTableName(schema);
    if ( excludeTablePredicates.stream()
      .anyMatch(it -> it.test(tableName)) )
    {
      return Optional.empty();
    }
    List<String> lines = Splitter.on("\n")
      .splitToList(schema);
    Map<String, String> columnToTypes = new HashMap<>();
    for ( String line : lines )
    {
      if ( line.startsWith("CREATE TABLE") )
      {
        continue;
      }
      if ( line.startsWith(")") )
      {
        break;
      }
      String[] parts = line.trim()
        .replace(",", "")
        .split(" ");
      columnToTypes.put(parts[0], parts[1]);
    }
    long numberOfRows = getNumberOfRows(dbClient, tableName);
    int numberOfIndexes = tableNamesToNumberOfIndexes.getOrDefault(tableName, new LongAdder())
      .intValue();
    return Optional.of(new Table(schema, tableName, numberOfRows, numberOfIndexes, columnToTypes));
  }

  private ArrayListMultimap<String, Table> buildTablesToTablesMap(DatabaseClient dbClient,
                                                                  Map<String, LongAdder> tableNamesToNumberOfIndexes,
                                                                  List<String> ddl)
  {
    ArrayListMultimap<String, Table> tablesToTables = ArrayListMultimap.create();
    StreamEx.of(ddl)
      .filter(DatabaseFactory::isCreateTableLine)
      .filter(tableLine -> tableLine.contains("INTERLEAVE IN PARENT "))
      .map(tableLine -> schemaToTable(dbClient, tableNamesToNumberOfIndexes, tableLine))
      .filter(Optional::isPresent)
      .map(Optional::get)
      .forEach(table ->
      {
        String parentTableName = StringUtils
          .substringBetween(table.schema, "INTERLEAVE IN PARENT ", " ON ");
        tablesToTables.put(parentTableName, table);
      });
    return tablesToTables;
  }

  private Map<String, LongAdder> buildTablesToIndexesMap(List<String> ddl)
  {
    Map<String, LongAdder> tableNamesToNumberOfIndexes = new HashMap<>();
    StreamEx.of(ddl)
      .filter(DatabaseFactory::isCreateIndexLine)
      .forEach(indexLine ->
      {
        String noPrefix = indexLine.replace("CREATE UNIQUE INDEX ", "")
          .replace("CREATE INDEX ", "");
        String[] parts = noPrefix.split(" ON ");
        String tableName = StringUtils.substringBefore(parts[1], "(");
        tableNamesToNumberOfIndexes.computeIfAbsent(tableName, t -> new LongAdder())
          .increment();
      });
    return tableNamesToNumberOfIndexes;
  }

  private long getNumberOfRows(DatabaseClient dbClient, String tableName)
  {
    try ( ResultSet rs = dbClient.singleUse()
      .executeQuery(Statement.of("SELECT COUNT(*) FROM " + tableName)) )
    {
      return rs.next() ? rs.getLong(0) : 0;
    }
  }

  public Database newDatabase(SpannerService fromSpannerService, Database fromDatabase, SpannerService toSpannerService)
  {
    try
    {
      //This is existence check
      DatabaseAdminClient databaseAdminClient = toSpannerService
        .getDatabaseAdminClient();
      log.info("Dropping database {}:{}...", toSpannerService.getSpannerProperties()
        .getInstance(), toSpannerService.getSpannerProperties()
        .getDatabase());
      databaseAdminClient
        .dropDatabase(toSpannerService.getSpannerProperties()
            .getInstance(),
          toSpannerService.getSpannerProperties()
            .getDatabase());
    }
    catch ( SpannerException se )
    {
      //Suppress NOT_FOUND message as there is no other way of checking db existence
      if ( !se.getMessage()
        .contains("NOT_FOUND") )
      {
        throw se;
      }
    }
    log.info("Creating database {}:{} based on {}",
      toSpannerService.getSpannerProperties().getInstance(),
      toSpannerService.getSpannerProperties().getDatabase(),
      fromDatabase);
    toSpannerService.getDatabaseAdminClient()
      .createDatabase(toSpannerService.getSpannerProperties()
        .getInstance(), toSpannerService.getSpannerProperties()
        .getDatabase(), fromSpannerService.getDdl())
      .waitFor();
    return load(toSpannerService);
  }
}
