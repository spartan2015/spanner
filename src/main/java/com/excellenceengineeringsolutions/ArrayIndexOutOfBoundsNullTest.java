

package com.excellenceengineeringsolutions;

import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;

public class ArrayIndexOutOfBoundsNullTest
{
  public static void main(String[] args)
  {
    byte[][] ar = null;
    System.out.println(ar[0]);
  }

  @Test
  public void t(){

    //ByteArrayStreamer streamer = new ByteArrayStreamer(BaseEncoding.base16().decode("5AFE1E027591CF4B1B5110C7E73BBB2E24B20B82180F0B092D8AA7E5ED46311B7A698389ED495050EA21841854881AC1D2451F494BA02D14FA83209E6C0BDEBDE8D583E2FF2022C583072FE259F1B03E130D7AF0F10686EF0CF3F9BE79399870080336D244B300812CBD723DFDD682477AD53C7D5F32C22DE4038061F21C013A836C304F4DBA32FB262385EB8841C7136E2AE179E9A1A4BA5238420AE9FBFE902F84CC964B41A6509C2BE5B3E5C24A5ABAAAE049CF1D1C1622994AFA52F80F23A42B44CB86D3B4C1B864BE3808B0CDB0F0978DDEC6CE66015DF8C79DF38B2BF9F2526166613EB029AC6339C091A64CAE2A936B2A2CFC20A3B6F7673BC2BDF8AF91CD271A1D6BF7CF1CBF9FBF4B7A1886B5003B842857099F099DF8AA3A4C7CB95B87F18DF443FB5DE23B1974F371B8889F0FC96F556EA8A82BE15C1F28A282E5369E2DBEC3AF9F7DFA807DED2A6C9D5A0B9E6BC341CF933B1E918BFD1BA527C0A6F0922DF58131406DC447642C33E68E4FBC9E1869513A9A946EA896A832F558ECC32B13236DB1A729BEC028311FC6B912E36A9C7FE116"));

    byte[][] data = {generateBytes((byte)1,100), generateBytes((byte)2,100),generateBytes((byte)3,90)};
    int maxsize = 300;

    byte[] collapsed = collapseContainerData(data, maxsize);
    System.out.println(collapsed.length);
    System.out.println(Arrays.toString(collapsed));

    byte[][] source = expandContainerData(collapsed, maxsize);
    for ( byte[] a : source )
    {
      System.out.println(Arrays.toString(a));
    }
  }

  private byte[] generateBytes(byte v, int length)
  {
    byte[] result = new byte[length];
    for(int i = 0; i < length; i++){
      result[i]=v;
    }
    return result;
  }


  static byte[] collapseContainerData(@Nonnull byte[][] aacbData, int maxContainerDataSize)
  {
    byte[] tmp = new byte[maxContainerDataSize];

    int offset = 0;
    for ( byte[] anAacbData : aacbData )
    {
      int len = Math.min(anAacbData.length, MAX_ROW_LENGTH_DB);
      System.arraycopy(anAacbData, 0, tmp, offset, len);
      offset += len;
    }

    return Arrays.copyOfRange(tmp,0,offset);
  }

  public static byte[][] expandContainerData(@Nonnull byte[] aacbData, int maxContainerDataSize)
  {
    int blobsAmount = maxContainerDataSize / MAX_ROW_LENGTH_DB;
    byte[][] expanded = new byte[blobsAmount][];

    int offset = 0;
    int bytesToCopy = aacbData.length;
    for ( int i = 0; i < blobsAmount; i++ )
    {
      int len = Math.min(bytesToCopy, MAX_ROW_LENGTH_DB);
      expanded[i] = new byte[len];
      System.arraycopy(aacbData, offset, expanded[i], 0, len);
      bytesToCopy -= len;
      offset += len;
    }

    return expanded;
  }
  public static final int CONTAINER_LENGTH = 8000;
  private static final int MAX_ROW_LENGTH_DB = 100;
}
