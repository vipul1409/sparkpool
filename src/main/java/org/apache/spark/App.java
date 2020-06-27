package org.apache.spark;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception {
        SparkConnection sc = SparkConnectionPool.getPool().borrowObject();
        Integer statementId = sc.execute("1+1");
        System.out.println(sc.getResult(statementId));
        SparkConnectionPool.getPool().close();
    }
}
