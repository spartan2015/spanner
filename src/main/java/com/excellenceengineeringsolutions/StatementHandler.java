


package com.excellenceengineeringsolutions;

import com.excellenceengineeringsolutions.spannerjdbc.BitwiseReverser;
import com.excellenceengineeringsolutions.spannerjdbc.SpannerSequenceGenerator;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.*;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class StatementHandler
{

  public static Statement.Builder spannerQueryBuilder(String select){
    AtomicInteger paramIndex = new AtomicInteger(1);
    return spannerQueryBuilder(select,paramIndex);
  }
  public static Statement.Builder spannerQueryBuilder(String select, AtomicInteger index)
  {
    return Statement.newBuilder(spannerQueryParams(select,index));
  }

  public static InsertMutationHolder spannerInsertBuilder(String insert, Map<String, Map<String,Object>> defaultParameters) throws RuntimeException
  {
    String[] insertParts = parserInsert(insert);
    Mutation.WriteBuilder writer = Mutation.newInsertBuilder(insertParts[0]);
    Map<String, Object> paramsInInsert = getParamsFromInsert(insertParts[1],insertParts[2]);
    Map<String,String> requiredParams = new HashMap<>();
    for ( String param : paramsInInsert.keySet() )
    {
      Object paramValue = paramsInInsert.get(param);
      if (!(paramValue instanceof String && ((String)paramValue).startsWith("@_")))
      {
        bindParam(writer, param, paramValue);
      }else{
        requiredParams.put((String)paramValue, param);
      }
    }
    return new InsertMutationHolder(insert,insertParts[0],writer,requiredParams,paramsInInsert,defaultParameters);
  }

  public static class UpdateMutationHolder{
    private String query;
    private String tableName;
    private List<String> keys;
    private Statement.Builder queryBuilder;
    private Map<String,Object> requiredSelectParams;
    private Map<String, Object> requiredUpdateParams;
    private Map<String, Object> indexToColumn;

    public UpdateMutationHolder(String query,
                                String tableName,
                                List<String> keys,
                                Statement.Builder queryBuilder,
                                Map<String,Object> requiredSelectParams,
                                Map<String, Object> requiredUpdateParams,
                                Map<String, Object> indexToColumn)
    {
      this.tableName=tableName;
      this.keys=keys;
      this.query = query;
      this.queryBuilder = queryBuilder;
      this.requiredUpdateParams = requiredUpdateParams;
      this.requiredSelectParams = requiredSelectParams;
      this.indexToColumn = indexToColumn;
    }

    public void bind(int paramNo, Object paramValue){
      String key = "@_" + paramNo;
      Map<String,Object> params = requiredSelectParams.containsKey(key) ? requiredSelectParams : indexToColumn;
      params.put(key,paramValue);
    }

    public void execute(DatabaseClient client){
      client.readWriteTransaction().run(new TransactionRunner.TransactionCallable<Void>()
      {
        @Nullable
        @Override
        public Void run(TransactionContext transaction) throws Exception
        {
          for(String param : requiredSelectParams.keySet()){
            StatementHandler.bind(queryBuilder,param,requiredSelectParams.get(param));
          }
          try(ResultSet rs = client.singleUse().executeQuery(queryBuilder.build()))
          {
            while(rs.next()){
              Mutation.WriteBuilder builder = Mutation.newUpdateBuilder(tableName);
              for(String key : keys){
                rs.getCurrentRowAsStruct().getColumnType(key);
                setToBuilderFromResultSet(builder,rs,key);
              }
              for(String column : requiredUpdateParams.keySet()){
                Object paramValue = requiredUpdateParams.get(column);
                if (paramValue instanceof String && ((String)paramValue).startsWith("@_")){
                  paramValue = indexToColumn.get(paramValue);
                }
                StatementHandler.bindParam(builder, column, paramValue);
              }
              transaction.buffer(builder.build());
            }
          }
          return null;
        }
      });
    }
  }

  public static void setToBuilderFromResultSet(Mutation.WriteBuilder builder, ResultSet rs, String fieldName)
  {
    Type.Code code = rs.getCurrentRowAsStruct().getColumnType(fieldName).getCode();
    Struct struct = rs.getCurrentRowAsStruct();
    switch ( code )
    {
      case BOOL:
        builder.set(fieldName).to(struct.getBoolean(fieldName));
        break;
      case DATE:
        builder.set(fieldName).to(struct.getDate(fieldName));
        break;
      case BYTES:
        builder.set(fieldName).to(struct.getBytes(fieldName));
        break;
      case INT64:
        builder.set(fieldName).to(struct.getLong(fieldName));
        break;
      case STRING:
        builder.set(fieldName).to(struct.getString(fieldName));
        break;
      case FLOAT64:
        builder.set(fieldName).to(struct.getDouble(fieldName));
        break;
      case TIMESTAMP:
        builder.set(fieldName).to(struct.getTimestamp(fieldName));
        break;
      default:
        break;
    }
  }

  public static UpdateMutationHolder spannerUpdateBuilder(List<String> selectKeys,String updateQuery) throws RuntimeException{
    String[] updateParts = parserUpdate(updateQuery);
    AtomicInteger paramIndex = new AtomicInteger();
    paramIndex.set(1);
    Map<String,Object> updateSetMapHolder = getUpdateSetMap(updateParts[1], paramIndex);
    int fromParamIndex=paramIndex.get();
    Statement.Builder queryBuilder = spannerQueryBuilder("select " + join(selectKeys) + " from " + updateParts[0] + " where " + updateParts[2], paramIndex);
    Map<String,Object> requiredSelectParams = new HashMap();
    for(int i = fromParamIndex; i < paramIndex.get(); i++){
      requiredSelectParams.put("@_"+i,null);
    }
    return new UpdateMutationHolder(updateQuery, updateParts[0],selectKeys, queryBuilder, requiredSelectParams,updateSetMapHolder, new HashMap());
  }

  private static String join(List<String> selectKeys)
  {
    StringBuilder sb = new StringBuilder();
    for(String s : selectKeys){
      sb.append(s).append(",");
    }
    sb.deleteCharAt(sb.length()-1);
    return sb.toString();
  }

  private static Map<String,Object> getUpdateSetMap(String updatePart, AtomicInteger paramIndex)
  {
    Map<String,Object> updateMap = new HashMap<>();
    for(String fieldSet : updatePart.split(",")){
      String[] fieldSetParts = fieldSet.split("=");
      Object value = parseValue(fieldSetParts[1]);
      updateMap.put(fieldSetParts[0].toUpperCase(), "?".equals(value) ? "@_" + paramIndex.getAndIncrement() : value);
    }
    return updateMap;
  }

  private static final String tableNameRegExp = "[a-z_A-Z0-9]+";
  private static Pattern UPDATE_REGEXP = Pattern.compile("(?i)update[ \n]+("+tableNameRegExp+")"+"[ \n]+SET[ \n]+"+"(.+)"+"[ \n]+(?:WHERE[ \n]+" + "(.*))");

  private static String[] parserUpdate(String updateQuery) throws RuntimeException
  {
    Matcher matcher = UPDATE_REGEXP.matcher(updateQuery);
    if (matcher.find()){
      return new String[]{matcher.group(1),matcher.group(2),matcher.group(3)};
    }else{
      throw new RuntimeException(String.format("Could not extract table name from query [%s]",updateQuery));
    }
  }

  private static String spannerQueryParams(String select){
    AtomicInteger paramIndex = new AtomicInteger();
    paramIndex.set(1);
    return spannerQueryParams(select, paramIndex);
  }
  private static String spannerQueryParams(String select, AtomicInteger paramIndex)
  {
    StringBuilder sb = new StringBuilder(select.length());
    int fromIndex = 0;
    int foundIndex = -1;
    do
    {
      foundIndex = select.indexOf("?", fromIndex);
      sb.append(select.substring(fromIndex, foundIndex == -1 ? select.length() : foundIndex));
      if ( foundIndex != -1 )
      {
        sb.append("@_" + paramIndex.getAndIncrement());
        fromIndex = foundIndex + 1;
      }
    } while ( foundIndex != -1 );
    return sb.toString();
  }

  public static <T> void bind(com.google.cloud.spanner.Statement.Builder builder, int paramNo, Object paramValue, Class<T> clazz)
  {
    String param = "@_" + paramNo;
    bind(builder, param, paramValue, clazz);
  }

  private static <T> void bind(Statement.Builder builder, String param, Object paramValue, Class<T> clazz)
  {
    if ( paramValue == null )
    {
      throw new IllegalArgumentException(String.format("Required param value for [%s] is missing.", param));
    }
    if ( clazz == Boolean.class )
    {
      builder.bind(param).to((Boolean) paramValue);
    } else if ( clazz == Float.class )
    {
      builder.bind(param).to((Float) paramValue);
    } else if ( clazz == Double.class )
    {
      builder.bind(param).to((Double) paramValue);
    } else if ( clazz == Number.class )
    {
      builder.bind(param).to(((Number) paramValue).longValue());
    } else if ( clazz == String.class )
    {
      builder.bind(param).to((String) paramValue);
    } else if ( clazz == ByteArray.class )
    {
      builder.bind(param).to((ByteArray) paramValue);
    } else if ( clazz == Timestamp.class )
    {
      builder.bind(param).to((Timestamp) paramValue);
    } else if ( clazz == Date.class )
    {
      builder.bind(param).to((Date) paramValue);
    } else
    {
      throw new IllegalArgumentException(String.format("Type [%s] is not supported",
        paramValue.getClass()));
    }
  }

  public static void bind(Statement.Builder builder, int paramNo, Object paramValue)
  {
    String param = "@_" + paramNo;
    bind(builder, param, paramValue);
  }

  public static void bind(Statement.Builder builder,  String param, Object paramValue)
  {
    if ( paramValue == null )
    {
      throw new IllegalArgumentException(String.format("Required param value for [%s] is missing.", param));
    }
    if ( paramValue instanceof Boolean )
    {
      builder.bind(param).to((Boolean) paramValue);
    } else if ( paramValue instanceof Float )
    {
      builder.bind(param).to((Float) paramValue);
    } else if ( paramValue instanceof Double )
    {
      builder.bind(param).to((Double) paramValue);
    } else if ( paramValue instanceof Number )
    {
      builder.bind(param).to(((Number) paramValue).longValue());
    } else if ( paramValue instanceof String )
    {
      builder.bind(param).to((String) paramValue);
    } else if ( paramValue instanceof ByteArray )
    {
      builder.bind(param).to((ByteArray) paramValue);
    } else if ( paramValue instanceof Timestamp )
    {
      builder.bind(param).to((Timestamp) paramValue);
    } else if ( paramValue instanceof Date )
    {
      builder.bind(param).to((Date) paramValue);
    } else
    {
      throw new IllegalArgumentException(String.format("Type [%s] is not supported",
        paramValue.getClass()));
    }
  }

  public void bind(Mutation.WriteBuilder builder, Map<String, Object> parameters)
  {
    for ( String param : parameters.keySet() )
    {
      Object paramValue = parameters.get(param);
      bindParam(builder, param, paramValue);
    }
  }

  private static void bindParam(Mutation.WriteBuilder builder, String param, Object paramValue)
  {
    if (paramValue == null){
      return;
    }
    if ( paramValue instanceof Boolean )
    {
      builder.set(param).to((Boolean) paramValue);
    }
    else if ( paramValue instanceof Float )
    {
      builder.set(param).to((Float) paramValue);
    }
    else if ( paramValue instanceof Double )
    {
      builder.set(param).to((Double) paramValue);
    }
    else if ( paramValue instanceof Number )
    {
      builder.set(param).to(((Number) paramValue).longValue());
    }
    else if ( paramValue instanceof String )
    {
      builder.set(param).to((String) paramValue);
    }
    else if ( paramValue instanceof ByteArray )
    {
      builder.set(param).to((ByteArray) paramValue);
    }
    else if ( paramValue instanceof Timestamp )
    {
      builder.set(param).to((Timestamp) paramValue);
    }
    else if ( paramValue instanceof Date )
    {
      builder.set(param).to((Date) paramValue);
    }
    else
    {
      throw new IllegalArgumentException(String.format("Type [%s] is not supported",
        paramValue.getClass()));
    }
  }

  public static class InsertMutationHolder
  {
    private String query;
    private String tableName;
    private Mutation.WriteBuilder mutationBuilder;
    private Map<String, String> requiredParams;
    private Map<String, Object> paramsInInsert;
    private Map<String, Map<String,Object>> defaultParameters;

    public InsertMutationHolder(String query, String tableName, Mutation.WriteBuilder mutationBuilder, Map<String, String> requiredParams,
                                Map<String, Object> paramsInInsert, Map<String, Map<String,Object>> defaultParameters)
    {
      this.query = query;
      this.tableName = tableName;
      this.mutationBuilder = mutationBuilder;
      this.requiredParams = requiredParams;
      this.paramsInInsert = paramsInInsert;
      this.defaultParameters = defaultParameters;
    }

    public void bind(int paramNo, Object paramValue){
      String key = "@_" + paramNo;
      bindParam(mutationBuilder,requiredParams.get(key), paramValue);
      requiredParams.remove(key);
    }
    public Mutation build(){
      if (!requiredParams.isEmpty()){
        throw new IllegalStateException("missing required params: " + requiredParams + " in original query: " + query);
      }
      for(String defaultParam : defaultParameters.get(tableName).keySet()){
        if (!paramsInInsert.containsKey(defaultParam)){
          bindParam(mutationBuilder,defaultParam,defaultParameters.get(tableName).get(defaultParam));
        }
      }
      return mutationBuilder.build();
    }
  }

  public static Map<String,Object> getParamsFromInsert(String columnsGroup, String valueGroup)
  {
    Map<String,Object> result = new HashMap<>();
    String[] columns = columnsGroup.split(",");
    Object[] values = parseValueGroup(valueGroup);
    int colIndex = 1;
    for(int i = 0; i < columns.length;i++){
       result.put(columns[i].toUpperCase(), ("?".equals(values[i])) ? "@_"+(colIndex++) : values[i]);
    }
    return result;
  }

  private static Object[] parseValueGroup(String valueGroup)
  {
    boolean inString = false;
    List<Object> result = new ArrayList<>();
    char prevChar = 0;
    for(int from=0,i=0; i<valueGroup.length() ; i++){
      char currentChar = valueGroup.charAt(i);
      if ( currentChar =='\'' && prevChar !='\\' ) {
        inString=!inString;
      }
      boolean atEnd = i == valueGroup.length() - 1;
      if ( (currentChar ==',' && !inString) || atEnd ){
        String value = valueGroup.substring(from, atEnd ? i + 1 : i).trim();

        result.add(parseValue(value));
        from = i+1;
      }
      prevChar = currentChar;
    }

    return result.toArray();
  }

  private static Object parseValue(String value)
  {
    if (value.length() == 0){
      return null;
    }else if(value.startsWith("'")){
      return value.substring(1,value.length()-1);
    }else if (value.matches("\\d+")){
      return Long.parseLong(value);
    }else{
      return value;
    }
  }


  private static final String colGroup = "\\s*\\(([^)]+)\\)";
  private static final String valGroup = "\\s+values\\s*\\((.+)\\)";
  static Pattern EXTRACT_TABLE_NAME_FROM_INSERT = Pattern.compile("(?i)INSERT[ ]+INTO[ ]+("+tableNameRegExp+")"+colGroup+valGroup);

  private static String[] parserInsert(String insert) throws RuntimeException
  {
    Matcher matcher = EXTRACT_TABLE_NAME_FROM_INSERT.matcher(insert);
    if (matcher.find()){
      return new String[]{matcher.group(1),matcher.group(2),matcher.group(3)};
    }else{
      throw new RuntimeException(String.format("Could not extract table name from query [%s]",insert));
    }
  }

  Map<String, Map<String,Object>> defaultParameters = new HashMap<>();
  {
    Map<String, Object> orderDefaults = new HashMap();
    orderDefaults.put("a",new Callable<Long>(){

      @Override
      public Long call() throws Exception
      {
        return getSeq;
      }
    });

    defaultParameters.put("a", orderDefaults);
  }

  @Test
  public void t1() throws Exception
  {
    {
      assertEquals("select * from a where c1=@_1 and c2=@_2",spannerQueryParams("select * from a where c1=? and c2=?", new AtomicInteger()));
    }
    {
      String[] parts = parserInsert("INsERT  IntO  myTable   (column1, col2, col_3, col_4) values ('a,B(())),c\\'sd',23,2019/01/02,'','',?)");
      assertEquals("myTable", parts[0]);
      assertEquals("column1, col2, col_3, col_4", parts[1]);
      assertEquals("'a,B(())),c\\'sd',23,2019/01/02,'','',?", parts[2]);
    }
    {
      Object[] result = parseValueGroup("'a,B,c\\'sd' ,23 ,2019/01/02 , '', '',?");
      assertEquals(6,result.length);
      assertEquals("a,B,c\\'sd", result[0]);
      assertEquals(Long.valueOf(23), result[1]);
      assertEquals("2019/01/02", result[2]);
      assertEquals("", result[3]);
      assertEquals("", result[4]);
      assertEquals("?", result[5]);
    }
    {
      InsertMutationHolder mutationHolder = spannerInsertBuilder("insert into a(c1,c2) values(?,?)", defaultParameters);
      assertEquals("c1",mutationHolder.requiredParams.get("@_1"));
      assertEquals("c2",mutationHolder.requiredParams.get("@_2"));
      mutationHolder.bind(1, "vc1");
      mutationHolder.bind(2, "vc2");
      assertTrue(mutationHolder.requiredParams.isEmpty());
      Mutation mutation = mutationHolder.build();
      assertEquals(Value.string("vc1"),mutation.asMap().get("c1"));
      assertEquals(Value.string("vc2"),mutation.asMap().get("c2"));
    }
    {
      InsertMutationHolder mutationHolder = spannerInsertBuilder("insert into a(c1,c2,c3,c4) values('asd',?,23,?)", defaultParameters);
      assertEquals("c2",mutationHolder.requiredParams.get("@_1"));
      assertEquals("c4",mutationHolder.requiredParams.get("@_2"));
      mutationHolder.bind(1, "vc2v");
      mutationHolder.bind(2, "vc4v");
      assertTrue(mutationHolder.requiredParams.isEmpty());
      Mutation mutation = mutationHolder.build();
      assertEquals("a",mutation.getTable());
      assertEquals(Value.string("asd"),mutation.asMap().get("c1"));
      assertEquals(Value.string("vc2v"),mutation.asMap().get("c2"));
      assertEquals(Value.int64(23),mutation.asMap().get("c3"));
      assertEquals(Value.string("vc4v"),mutation.asMap().get("c4"));
    }

  }

  private void insertOrder() throws RuntimeException
  {
    InsertMutationHolder insert = spannerInsertBuilder("", defaultParameters);
    insert.bind(1, new java.util.Date().getTime());
    insert.bind(2, new java.util.Date().getTime());
    insert.bind(3, 1);
    insert.bind(4, "filename");
    insert.bind(5, "username");
    insert.bind(6, "A");
    insert.bind(7, 1);
    insert.bind(8, "A");
    insert.bind(9, "farm specific");


    getClient().write(Arrays.asList(insert.build()));
  }


  public DatabaseClient getClient()
  {
    String projectId = "a-cloud-spanner";
    SpannerOptions.Builder spannerOptionsBuilder = SpannerOptions.newBuilder()
      .setProjectId(projectId)
      .setSessionPoolOption(
        SessionPoolOptions.newBuilder()
          .setFailIfPoolExhausted()
          .setMaxSessions(100)
          .setMinSessions(1)
          .setWriteSessionsFraction(0.5f)
          .build())
      .setNumChannels(1)
      .setTransportOptions(GrpcTransportOptions.newBuilder()
        .setExecutorFactory(new GrpcTransportOptions.ExecutorFactory<ScheduledExecutorService>()
        {
          private final ScheduledThreadPoolExecutor service = new ScheduledThreadPoolExecutor(2);

          @Override
          public ScheduledExecutorService get()
          {
            return service;
          }

          @Override
          public void release(ScheduledExecutorService service)
          {
            service.shutdown();
          }
        })
        .build());

    SpannerOptions options = spannerOptionsBuilder.build();
    Spanner spanner = null;

    spanner = options.getService();
    String instanceId = "eu-instance";
    String databaseId = "vasile";
    DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);

    DatabaseClient dbClient = spanner.getDatabaseClient(db);
    DatabaseAdminClient admin = spanner.getDatabaseAdminClient();

    return dbClient;
  }

  private volatile SpannerSequenceGenerator spannerSequenceGenerator;

  public SpannerSequenceGenerator getSequence() throws RuntimeException
  {
    if ( spannerSequenceGenerator == null )
    {
      synchronized ( SpannerSequenceGenerator.class )
      {
        if ( spannerSequenceGenerator == null )
        {
          spannerSequenceGenerator = new SpannerSequenceGenerator(
            getSpannerClientProvider()
          );
        }
      }
    }
    return spannerSequenceGenerator;
  }
  public long getNextSequenceValue(String sequenceName) throws FrwException
  {
    return BitwiseReverser.reverseBits(getSequence().getNextValue(sequenceName));
  }

}
*/
