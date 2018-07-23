/* Copyright (c) Nokia Siemens Networks 2007 All Rights Reserved
   The reproduction, transmission or use of this document or its contents
   is not permitted without express written authority. Offenders will be
   liable for damages. All rights, including rights created by patent grant
   or registration of a utility model or design, are reserved.
   Technical modifications possible.
   Technical specifications and features are binding only insofar as they
   are specifically and expressly agreed upon in a written contract.
*/

package com.excellenceengineeringsolutions.spannerjdbc;

import com.google.cloud.spanner.ResultSet;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

public class SpannerReaderUtils
{
  public static byte[] readBytes(ResultSet rs, String index)
  {
    return rs.isNull(index) ? null : rs.getBytes(index).toByteArray();
  }

  public static byte[] readBytes(ResultSet rs, int index)
  {
    return rs.isNull(index) ? null : rs.getBytes(index).toByteArray();
  }

  public static Timestamp readTimestamp(ResultSet rs, int index)
  {
    return rs.isNull(index) ? null : rs.getTimestamp(index).toSqlTimestamp();
  }

  public static Timestamp readTimestamp(ResultSet rs, String name)
  {
    return rs.isNull(name) ? null : rs.getTimestamp(name).toSqlTimestamp();
  }

  public static Time readTime(ResultSet rs, int index)
  {
    return rs.isNull(index) ? null : toSqlTime(rs.getTimestamp(index));
  }

  public static Time readTime(ResultSet rs, String name)
  {
    return rs.isNull(name) ? null : toSqlTime(rs.getTimestamp(name));
  }

  public static Date readDate(ResultSet rs, int index)
  {
    return rs.isNull(index) ? null : toSqlDate(rs.getTimestamp(index));
  }

  public static Date readDate(ResultSet rs, String name)
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

  public static Long readLongWithDefault(ResultSet rs, String name, Long defaultValue)
  {
    return rs.isNull(name) ? defaultValue : rs.getLong(name);
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
    return rs.isNull(index) ? Collections.emptyList() : toSqlTimestampList(rs.getTimestampList(index));
  }

  public static List<Timestamp> readTimestampList(ResultSet rs, String name)
  {
    return rs.isNull(name) ? Collections.emptyList() : toSqlTimestampList(rs.getTimestampList(name));
  }

  public static List<Time> readTimeList(ResultSet rs, int index)
  {
    return rs.isNull(index) ? Collections.emptyList() : toSqlTimeList(rs.getTimestampList(index));
  }

  public static List<Time> readTimeList(ResultSet rs, String name)
  {
    return rs.isNull(name) ? Collections.emptyList() : toSqlTimeList(rs.getTimestampList(name));
  }

  public static List<Date> readDateList(ResultSet rs, int index)
  {
    return rs.isNull(index) ? Collections.emptyList() : toSqlDateList(rs.getTimestampList(index));
  }

  public static List<Date> readDateList(ResultSet rs, String name)
  {
    return rs.isNull(name) ? Collections.emptyList() : toSqlDateList(rs.getTimestampList(name));
  }

  public static List<Integer> readIntList(ResultSet rs, int index)
  {
    return rs.isNull(index) ? Collections.emptyList() : toIntList(rs.getLongList(index));
  }

  public static List<Integer> toIntList(List<Long> longList)
  {
    List<Integer> result = new ArrayList();
    for ( Long l : longList )
    {
      result.add(l.intValue());
    }
    return result;
  }

  public static List<Integer> readIntList(ResultSet rs, String name)
  {
    return rs.isNull(name) ? Collections.emptyList() : toIntList(rs.getLongList(name));
  }

  public static List<Long> readLongList(ResultSet rs, int index)
  {
    return rs.isNull(index) ? Collections.emptyList() : rs.getLongList(index);
  }

  public static List<Long> readLongList(ResultSet rs, String name)
  {
    return rs.isNull(name) ? Collections.emptyList() : rs.getLongList(name);
  }

  public static List<Float> readFloatList(ResultSet rs, int index)
  {
    return rs.isNull(index) ? Collections.emptyList() : toFloatList(rs.getDoubleList(index));
  }

  public static List<Float> toFloatList(List<Double> doubleList)
  {
    List<Float> result = new ArrayList<Float>();
    for ( Double d : doubleList )
    {
      result.add(d.floatValue());
    }
    return result;
  }

  public static List<Float> readFloatList(ResultSet rs, String name)
  {
    return rs.isNull(name) ? Collections.emptyList() : toFloatList(rs.getDoubleList(name));
  }

  public static List<Double> readDoubleList(ResultSet rs, int index)
  {
    return rs.isNull(index) ? Collections.emptyList() : rs.getDoubleList(index);
  }

  public static List<Double> readDoubleList(ResultSet rs, String name)
  {
    return rs.isNull(name) ? Collections.emptyList() : rs.getDoubleList(name);
  }

  public static List<String> readStringList(ResultSet rs, int index)
  {
    return rs.isNull(index) ? Collections.emptyList() : rs.getStringList(index);
  }

  public static List<String> readStringList(ResultSet rs, String column)
  {
    return rs.isNull(column) ? Collections.emptyList() : rs.getStringList(column);
  }

  public static List<Date> toSqlDateList(List<com.google.cloud.Timestamp> timestampList)
  {
    if ( timestampList == null )
    {
      return Collections.emptyList();
    }
    List<Date> result = new ArrayList();
    for ( com.google.cloud.Timestamp timestamp : timestampList )
    {
      result.add(new Date(timestamp.toSqlTimestamp().getTime()));
    }
    return result;
  }

  public static List<Time> toSqlTimeList(List<com.google.cloud.Timestamp> timestampList)
  {
    if ( timestampList == null )
    {
      return Collections.emptyList();
    }
    List<Time> result = new ArrayList();
    for ( com.google.cloud.Timestamp timestamp : timestampList )
    {
      result.add(new Time(timestamp.toSqlTimestamp().getTime()));
    }
    return result;
  }

  public static List<Timestamp> toSqlTimestampList(List<com.google.cloud.Timestamp> timestampList)
  {
    if ( timestampList == null )
    {
      return Collections.emptyList();
    }
    List<Timestamp> result = new ArrayList();
    for ( com.google.cloud.Timestamp timestamp : timestampList )
    {
      result.add(timestamp.toSqlTimestamp());
    }
    return result;
  }

  public static Date toSqlDate(com.google.cloud.Timestamp timestamp)
  {
    return new Date(timestamp.toSqlTimestamp().getTime());
  }

  public static Time toSqlTime(com.google.cloud.Timestamp timestamp)
  {
    return new Time(timestamp.toSqlTimestamp().getTime());
  }

  public static Timestamp toSqlTimestamp(com.google.cloud.Timestamp timestamp)
  {
    return timestamp.toSqlTimestamp();
  }

  public static long[] toSpannerArray(int[] someArrayOfInt)
  {
    if ( someArrayOfInt == null )
    {
      return null;
    }
    long[] result = new long[someArrayOfInt.length];
    for ( int i = 0; i < someArrayOfInt.length; i++ )
    {
      result[i] = someArrayOfInt[i];
    }
    return result;
  }

  public static double[] toSpannerArray(float[] someArrayOfFloat)
  {
    if ( someArrayOfFloat == null )
    {
      return null;
    }
    double[] result = new double[someArrayOfFloat.length];
    for ( int i = 0; i < someArrayOfFloat.length; i++ )
    {
      result[i] = someArrayOfFloat[i];
    }
    return result;
  }

  public static Iterable<com.google.cloud.Timestamp> toSpannerDateList(java.util.Date[] someArrayOfDate)
  {
    if ( someArrayOfDate == null )
    {
      return Collections.EMPTY_LIST;
    }
    List<com.google.cloud.Timestamp> result = new ArrayList();
    for ( java.util.Date date : someArrayOfDate )
    {
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      calendar.set(Calendar.HOUR, 0);
      calendar.set(Calendar.MINUTE, 0);
      calendar.set(Calendar.SECOND, 0);
      calendar.set(Calendar.MILLISECOND, 0);
      result.add(com.google.cloud.Timestamp.of(calendar.getTime()));
    }
    return result;
  }

  public static Iterable<com.google.cloud.Timestamp> toSpannerTimeList(java.util.Date[] someArrayOfDate)
  {
    if ( someArrayOfDate == null )
    {
      return Collections.EMPTY_LIST;
    }
    List<com.google.cloud.Timestamp> result = new ArrayList();
    for ( java.util.Date date : someArrayOfDate )
    {
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      calendar.set(1970, 0, 1);
      calendar.set(Calendar.MILLISECOND, 0);
      result.add(com.google.cloud.Timestamp.of(calendar.getTime()));
    }
    return result;
  }

  public static Iterable<com.google.cloud.Timestamp> toSpannerDateTimeList(java.util.Date[] someArrayOfDate)
  {
    if ( someArrayOfDate == null )
    {
      return Collections.EMPTY_LIST;
    }
    List<com.google.cloud.Timestamp> result = new ArrayList();
    for ( java.util.Date date : someArrayOfDate )
    {
      result.add(com.google.cloud.Timestamp.of(date));
    }
    return result;
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
    return rs.isNull(index) ? Collections.emptyList() : rs.getBooleanList(index);
  }

  public static List<Boolean> readBooleanList(ResultSet rs, String column)
  {
    return rs.isNull(column) ? Collections.emptyList() : rs.getBooleanList(column);
  }

  public static com.google.cloud.Timestamp toSpannerDate(java.util.Date date)
  {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.set(Calendar.HOUR, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    return com.google.cloud.Timestamp.of(calendar.getTime());
  }

  public static com.google.cloud.Timestamp toSpannerTime(Date date)
  {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.set(1970, 0, 1);
    calendar.set(Calendar.MILLISECOND, 0);
    return com.google.cloud.Timestamp.of(calendar.getTime());
  }

  public static com.google.cloud.Timestamp toSpannerDateTime(Date date)
  {
    return com.google.cloud.Timestamp.of(date);
  }

  public static Long toLong(long v)
  {
    return Long.valueOf(v);
  }

  public static Double toDouble(float value)
  {
    return Double.valueOf(value);
  }
}
