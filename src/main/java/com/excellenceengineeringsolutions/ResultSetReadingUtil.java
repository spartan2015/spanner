

package com.excellenceengineeringsolutions;

import com.google.cloud.spanner.ResultSet;
import org.junit.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

public class ResultSetReadingUtil
{
  public static byte[] readBytes(ResultSet rs, int index)
  {
    return rs.isNull(index) ? null : rs.getBytes(index).toByteArray();
  }

  public static java.sql.Timestamp readTimestamp(ResultSet rs, int index)
  {
    return rs.isNull(index) ? null : rs.getTimestamp(index).toSqlTimestamp();
  }

  public static java.sql.Timestamp readTimestamp(ResultSet rs, String name)
  {
    return rs.isNull(name) ? null : rs.getTimestamp(name).toSqlTimestamp();
  }

  public static java.sql.Time readTime(ResultSet rs, int index)
  {
    return rs.isNull(index) ? null : toSqlTime(rs.getTimestamp(index));
  }

  public static java.sql.Time readTime(ResultSet rs, String name)
  {
    return rs.isNull(name) ? null : toSqlTime(rs.getTimestamp(name));
  }

  public static java.sql.Date readDate(ResultSet rs, int index)
  {
    return rs.isNull(index) ? null : toSqlDate(rs.getTimestamp(index));
  }

  public static java.sql.Date readDate(ResultSet rs, String name)
  {
    return rs.isNull(name) ? null : toSqlDate(rs.getTimestamp(name));
  }

  public static Integer readInt(ResultSet rs, int index)
  {
    return rs.isNull(index) ? null : (int) rs.getLong(index);
  }

  public static Integer readInt(ResultSet rs, String name)
  {
    return rs.isNull(name) ? null : (int) rs.getLong(name);
  }

  public static Long readLong(ResultSet rs, int index)
  {
    return rs.isNull(index) ? null : rs.getLong(index);
  }

  public static Long readLong(ResultSet rs, String name)
  {
    return rs.isNull(name) ? null : rs.getLong(name);
  }

  public static Float readFloat(ResultSet rs, int index)
  {
    return rs.isNull(index) ? null : (float) rs.getDouble(index);
  }

  public static Float readFloat(ResultSet rs, String name)
  {
    return rs.isNull(name) ? null : (float) rs.getDouble(name);
  }

  public static Double readDouble(ResultSet rs, int index)
  {
    return rs.isNull(index) ? null : rs.getDouble(index);
  }

  public static Double readDouble(ResultSet rs, String name)
  {
    return rs.isNull(name) ? null : rs.getDouble(name);
  }

  public static String readString(ResultSet rs, int index)
  {
    return rs.isNull(index) ? null : rs.getString(index);
  }

  public static String readString(ResultSet rs, String column)
  {
    return rs.isNull(column) ? null : rs.getString(column);
  }

  public static List<Timestamp> readTimestampList(ResultSet rs, int index)
  {
    return rs.isNull(index) ? Collections.<java.sql.Timestamp>emptyList() : toSqlTimestampList(rs.getTimestampList(index));
  }

  public static List<java.sql.Timestamp> readTimestampList(ResultSet rs, String name)
  {
    return rs.isNull(name) ? Collections.<java.sql.Timestamp>emptyList() : toSqlTimestampList(rs.getTimestampList(name));
  }

  public static List<java.sql.Time> readTimeList(ResultSet rs, int index)
  {
    return rs.isNull(index) ? Collections.<java.sql.Time>emptyList() : toSqlTimeList(rs.getTimestampList(index));
  }

  public static List<java.sql.Time> readTimeList(ResultSet rs, String name)
  {
    return rs.isNull(name) ? Collections.<java.sql.Time>emptyList() : toSqlTimeList(rs.getTimestampList(name));
  }

  public static List<java.sql.Date> readDateList(ResultSet rs, int index)
  {
    return rs.isNull(index) ? Collections.<java.sql.Date>emptyList() : toSqlDateList(rs.getTimestampList(index));
  }

  public static List<java.sql.Date> readDateList(ResultSet rs, String name)
  {
    return rs.isNull(name) ? Collections.<java.sql.Date>emptyList() : toSqlDateList(rs.getTimestampList(name));
  }

  public static List<Integer> readIntList(ResultSet rs, int index)
  {
    return rs.isNull(index) ? Collections.<Integer>emptyList() : toIntList(rs.getLongList(index));
  }

  private static List<Integer> toIntList(List<Long> longList)
  {
    List<Integer> result = new ArrayList();
    for(Long l : longList){
      result.add(l.intValue());
    }
    return result;
  }

  public static List<Integer> readIntList(ResultSet rs, String name)
  {
    return rs.isNull(name) ? Collections.<Integer>emptyList() : toIntList(rs.getLongList(name));
  }

  public static List<Long> readLongList(ResultSet rs, int index)
  {
    return rs.isNull(index) ? Collections.<Long>emptyList() : rs.getLongList(index);
  }

  public static List<Long> readLongList(ResultSet rs, String name)
  {
    return rs.isNull(name) ? Collections.<Long>emptyList() : rs.getLongList(name);
  }

  public static List<Float> readFloatList(ResultSet rs, int index)
  {
    return rs.isNull(index) ? Collections.<Float>emptyList() : toFloatList(rs.getDoubleList(index));
  }

  private static List<Float> toFloatList(List<Double> doubleList)
  {
    List<Float> result = new ArrayList<Float>();
    for(Double d : doubleList){
      result.add(d.floatValue());
    }
    return result;
  }

  public static List<Float> readFloatList(ResultSet rs, String name)
  {
    return rs.isNull(name) ? Collections.<Float>emptyList() : toFloatList(rs.getDoubleList(name));
  }

  public static List<Double> readDoubleList(ResultSet rs, int index)
  {
    return rs.isNull(index) ? Collections.<Double>emptyList() : rs.getDoubleList(index);
  }

  public static List<Double> readDoubleList(ResultSet rs, String name)
  {
    return rs.isNull(name) ? Collections.<Double>emptyList() : rs.getDoubleList(name);
  }

  public static List<String> readStringList(ResultSet rs, int index)
  {
    return rs.isNull(index) ? Collections.<String>emptyList() : rs.getStringList(index);
  }

  public static List<String> readStringList(ResultSet rs, String column)
  {
    return rs.isNull(column) ? Collections.<String>emptyList() : rs.getStringList(column);
  }

  public static Boolean readBoolean(ResultSet rs, int index)
  {
    return rs.isNull(index) ? null : rs.getBoolean(index);
  }

  public static Boolean readBoolean(ResultSet rs, String column)
  {
    return rs.isNull(column) ? null : rs.getBoolean(column);
  }

  public static List<Boolean> readBooleanList(ResultSet rs, int index)
  {
    return rs.isNull(index) ? Collections.<Boolean>emptyList() : rs.getBooleanList(index);
  }

  public static List<Boolean> readBooleanList(ResultSet rs, String column)
  {
    return rs.isNull(column) ? Collections.<Boolean>emptyList() : rs.getBooleanList(column);
  }

  public static java.util.List<java.sql.Date> toSqlDateList(java.util.List<com.google.cloud.Timestamp> timestampList)
  {
    if (timestampList==null)
    {
      return java.util.Collections.emptyList();
    }
    List<java.sql.Date> result = new ArrayList();
    for(com.google.cloud.Timestamp timestamp : timestampList){
      result.add(new java.sql.Date(timestamp.toSqlTimestamp().getTime()));
    }
    return result;
  }
  public static java.util.List<java.sql.Time> toSqlTimeList(java.util.List<com.google.cloud.Timestamp> timestampList)
  {
    if (timestampList==null)
    {
      return java.util.Collections.emptyList();
    }
    List<java.sql.Time> result = new ArrayList();
    for(com.google.cloud.Timestamp timestamp : timestampList){
      result.add(new java.sql.Time(timestamp.toSqlTimestamp().getTime()));
    }
    return result;
  }
  public static java.util.List<java.sql.Timestamp> toSqlTimestampList(java.util.List<com.google.cloud.Timestamp> timestampList)
  {
    if (timestampList==null)
    {
      return java.util.Collections.emptyList();
    }
    List<java.sql.Timestamp> result = new ArrayList();
    for(com.google.cloud.Timestamp timestamp : timestampList){
      result.add(timestamp.toSqlTimestamp());
    }
    return result;
  }

  public static java.sql.Date toSqlDate(com.google.cloud.Timestamp timestamp)
  {
    return new java.sql.Date(timestamp.toSqlTimestamp().getTime());
  }

  public static java.sql.Time toSqlTime(com.google.cloud.Timestamp timestamp)
  {
    return new java.sql.Time(timestamp.toSqlTimestamp().getTime());
  }

  public static java.sql.Timestamp toSqlTimestamp(com.google.cloud.Timestamp timestamp)
  {
    return timestamp.toSqlTimestamp();
  }
  private long[] toSpannerArray(int[] someArrayOfInt)
  {
    if (someArrayOfInt == null)
    {
      return null;
    }
    long[] result = new long[someArrayOfInt.length];
    for(int i = 0; i < someArrayOfInt.length; i++){
      result[i] = someArrayOfInt[i];
    }
    return result;
  }

  private double[] toSpannerArray(float[] someArrayOfFloat)
  {
    if (someArrayOfFloat == null)
    {
      return null;
    }
    double[] result = new double[someArrayOfFloat.length];
    for(int i = 0; i < someArrayOfFloat.length; i++){
      result[i] = someArrayOfFloat[i];
    }
    return result;
  }

  public static Iterable<com.google.cloud.Timestamp> toSpannerDateList(java.util.Date[] someArrayOfDate)
  {
    if (someArrayOfDate==null)
    {
      return java.util.Collections.EMPTY_LIST;
    }
    java.util.List<com.google.cloud.Timestamp> result = new java.util.ArrayList();
    for(java.util.Date date : someArrayOfDate){
      java.util.Calendar calendar = java.util.Calendar.getInstance();
      calendar.setTime(date);
      calendar.set(Calendar.HOUR,0);
      calendar.set(Calendar.MINUTE,0);
      calendar.set(Calendar.SECOND,0);
      calendar.set(Calendar.MILLISECOND,0);
      result.add(com.google.cloud.Timestamp.of(calendar.getTime()));
    }
    return result;
  }

  public static Iterable<com.google.cloud.Timestamp> toSpannerTimeList(Date[] someArrayOfDate)
  {
    if (someArrayOfDate==null)
    {
      return java.util.Collections.EMPTY_LIST;
    }
    java.util.List<com.google.cloud.Timestamp> result = new java.util.ArrayList();
    for(Date date : someArrayOfDate){
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      calendar.set(1970,0,1);
      calendar.set(Calendar.MILLISECOND,0);
      result.add(com.google.cloud.Timestamp.of(calendar.getTime()));
    }
    return result;
  }

  public static Iterable<com.google.cloud.Timestamp> toSpannerDateTimeList(Date[] someArrayOfDate)
  {
    if (someArrayOfDate==null)
    {
      return java.util.Collections.EMPTY_LIST;
    }
    java.util.List<com.google.cloud.Timestamp> result = new java.util.ArrayList();
    for(Date date : someArrayOfDate){
      result.add(com.google.cloud.Timestamp.of(date));
    }
    return result;
  }

  public static com.google.cloud.Timestamp toSpannerDate(java.util.Date date)
  {
    java.util.Calendar calendar = java.util.Calendar.getInstance();
    calendar.setTime(date);
    calendar.set(Calendar.HOUR,0);
    calendar.set(Calendar.MINUTE,0);
    calendar.set(Calendar.SECOND,0);
    calendar.set(Calendar.MILLISECOND,0);
    return com.google.cloud.Timestamp.of(calendar.getTime());
  }
  @Test
  public void testtoSpannerDate(){
    com.google.cloud.Timestamp time = toSpannerDate(Calendar.getInstance().getTime());
    System.out.println(time);
  }

  public static com.google.cloud.Timestamp toSpannerTime(Date date)
  {
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      calendar.set(1970,0,1);
      calendar.set(Calendar.MILLISECOND,0);
      return com.google.cloud.Timestamp.of(calendar.getTime());
  }

  public static com.google.cloud.Timestamp toSpannerDateTime(Date date)
  {
    return com.google.cloud.Timestamp.of(date);
  }
}
