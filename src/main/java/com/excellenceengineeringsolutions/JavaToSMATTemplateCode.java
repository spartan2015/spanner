

package com.excellenceengineeringsolutions;

import com.google.common.base.Charsets;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class JavaToSMATTemplateCode
{

  public static void main(String[] args) throws Exception
  {
    transform(getLines(Paths.get(args[0])));
  }

  @Test
  public void t12()
  {
    System.out.println(TimeUnit.MINUTES.toSeconds(1));
  }




  @Test
  public void getString(){
    StringBuilder sqlcmd = new StringBuilder();
    appender(sqlcmd);
    System.out.println(sqlcmd.toString());
  }

  private void appender(StringBuilder sqlcmd) {
    

  }

  private static void transform(String string){
    transform(Arrays.asList(string.split("\n")));
  }

  private static void transform(List<String> lines)
  {
    for(String line : lines ){
      System.out.print("sqlcmd.append(\"");
      System.out.print(line.replace("\"","\\\""));
      System.out.println("\");");
    }
  }

    private static void transformAsImport(String string){
        transformAsImport(Arrays.asList(string.split("\n")));
    }

    private static void transformAsImport(List<String> lines)
    {
        for(String line : lines ){
            System.out.print("cliche.getImports().appendln(\"");
            System.out.print(line.replace("\"","\\\""));
            System.out.println("\");");
        }
    }

  private static List<String> getLines(Path input) throws IOException
  {
    return Files.readAllLines(input, Charsets.ISO_8859_1);
  }

}
