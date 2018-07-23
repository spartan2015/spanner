

package com.excellenceengineeringsolutions;

/**
 * Created by eXpert on 6/13/2018.
 */
public class AppException extends Exception
{

  static class IFrwContainer{}
  static class IFrwComponentHandle{}

  public AppException(String msg)
  {

  }

  public AppException(IFrwContainer container,
                      IFrwComponentHandle handle,
                      String msg)
  {

  }
}
