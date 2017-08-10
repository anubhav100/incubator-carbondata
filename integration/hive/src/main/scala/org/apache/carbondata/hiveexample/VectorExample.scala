package org.apache.carbondata.hiveexample

import java.sql.{DriverManager, ResultSet, Statement}

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.hive.server.HiveEmbeddedServer2

object VectorExample extends App {


  private val driverName: String = "org.apache.hive.jdbc.HiveDriver"

  try {
    Class.forName(driverName)
  }
  catch {
    case classNotFoundException: ClassNotFoundException =>
      classNotFoundException.printStackTrace()
  }

  val hiveEmbeddedServer2 = new HiveEmbeddedServer2()
  hiveEmbeddedServer2.start()
  val port = hiveEmbeddedServer2.getFreePort
  val connection = DriverManager.getConnection(s"jdbc:hive2://localhost:$port/default", "", "")
  val statement: Statement = connection.createStatement

  println(s"============HIVE CLI IS STARTED ON PORT $port ==============")

  statement.execute("CREATE TABLE IF NOT EXISTS " + "HIVE_CARBON_EXAMPLE " +
                    " (ID int, NAME string,SALARY double)")

  statement
    .execute(
      "alter table HIVE_CARBON_EXAMPLE set FILEFORMAT INPUTFORMAT 'org.apache.carbondata.hive" +
      ".VectorizedCarbonInputFormat' OUTPUTFORMAT 'org.apache.carbondata.hive" +
      ".MapredCarbonOutputFormat' SERDE 'org.apache.carbondata.hive.CarbonHiveSerDe'")

  statement.execute("alter table HIVE_CARBON_EXAMPLE set LOCATION " +
                    "'hdfs://localhost:54310/user/hive/warehouse/default/hiveperf'")

  val resultSet: ResultSet = statement.executeQuery("DESC FORMATTED HIVE_CARBON_EXAMPLE")

  val rsmd = resultSet.getMetaData

  val columnCount: Int = resultSet.getMetaData.getColumnCount

  println("=========> " + columnCount)

  while (resultSet.next()){

    println("--------------------------------------------------")
    println(resultSet.getString(1) + " | " + resultSet.getString(2) + " | " + resultSet.getString(2))
  }
  println("--------------------------------------------------")

  statement.executeQuery("select NAME from HIVE_CARBON_EXAMPLE group by NAME")

  statement.close()
  hiveEmbeddedServer2.stop()

  Thread.sleep(2000)
  System.exit(0)

 /* val sparkSession = SparkSession
    .builder()
    .master("local")
    .appName("HiveExample")
    .enableHiveSupport()
    .getOrCreate()

  sparkSession.sql("DROP TABLE IF EXISTS hive_carbon").show()

  sparkSession.sql("CREATE TABLE IF NOT EXISTS hive_carbon(id int, name string) row format " +
                   "delimited fields terminated by ','").show()


  sparkSession
    .sql(
      "alter table hive_carbon set LOCATION " +
      "'hdfs://localhost:54310/user/hive/warehouse/default/hiveperf'")
    .show()

  sparkSession
    .sql(
      "alter table hive_carbon5 set FILEFORMAT INPUTFORMAT 'org.apache.carbondata.hive" +
      ".VectorizedCarbonInputFormat' OUTPUTFORMAT 'org.apache.carbondata.hive" +
      ".MapredCarbonOutputFormat' SERDE 'org.apache.carbondata.hive.CarbonHiveSerDe'")

  sparkSession.sql("DESC FORMATTED hive_carbon").show()*/

}
