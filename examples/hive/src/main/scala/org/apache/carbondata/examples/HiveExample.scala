package org.apache.carbondata.examples

import java.io.File
import java.sql.{Connection, DriverManager, ResultSet, SQLException, Statement}

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties


object HiveExample {

  private val driverName: String = "org.apache.hive.jdbc.HiveDriver"

  /**
   * @param args
   * @throws SQLException
   */
  @throws[SQLException]
  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/hive/target/store"
    val warehouse = s"$rootPath/examples/hive/target/warehouse"
    val metastoredb = s"$rootPath/examples/hive/target"
    val logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

    import org.apache.spark.sql.CarbonSession._

    val carbon = SparkSession
      .builder()
      .master("local")
      .appName("HiveExample")
      .config("carbon.sql.warehouse.dir", warehouse).enableHiveSupport()
      .getOrCreateCarbonSession(
        "hdfs://localhost:54310/opt/carbonStore")

    carbon.sql("""drop table if exists hive_carbon_example""".stripMargin)

    carbon
      .sql(
        """create table hive_carbon_example (id int,name string) stored by 'carbondata' """
          .stripMargin)

    carbon.sql(
      s"""
           LOAD DATA LOCAL INPATH '$rootPath/examples/hive/src/main/resources/dat.csv' into table
         hive_carbon_example
           """)
    carbon.sql("select * from hive_carbon_example").show()

    try {
      Class.forName(driverName)
    }
    catch {
      case classNotFoundException: ClassNotFoundException =>
        // TODO Auto-generated catch block
        classNotFoundException.printStackTrace()
        System.exit(1)
    }
    //replace " " here with the name of the user the queries should run as
    val con: Connection = DriverManager
      .getConnection("jdbc:hive2://localhost:10000/default", "", "")
    val stmt: Statement = con.createStatement
    logger.info("============HIVE CLI IS STARTED=============")

    stmt
      .execute(s"ADD JAR $rootPath/assembly/target/scala-2.11/carbondata_2.11-1.1" +
               s".0-incubating-SNAPSHOT-shade-hadoop2.7.2.jar ")
    stmt
      .execute(s"ADD JAR $rootPath/integration/hive/target/carbondata-hive-1.1" +
               s".0-incubating-SNAPSHOT.jar")

    stmt.execute("create table if not exists " + "hive_carbon_example " +
                 " (id int, name string)")

    stmt
      .execute(
        "alter table hive_carbon_example set FILEFORMAT INPUTFORMAT \"org.apache.carbondata." +
        "hive.MapredCarbonInputFormat\"OUTPUTFORMAT \"org.apache.carbondata.hive." +
        "MapredCarbonOutputFormat\"SERDE \"org.apache.carbondata.hive." +
        "CarbonHiveSerDe\" ")

    stmt
      .execute(
        "alter table hive_carbon_example set LOCATION " +
        "'hdfs://localhost:54310/opt/carbonStore/default/hive_carbon_example' ")

    stmt.execute("set hive.mapred.supports.subdirectories=true")
    stmt.execute("set mapreduce.input.fileinputformat.input.dir.recursive=true")

    val sql = "select id from hive_carbon_example"

    val res: ResultSet = stmt.executeQuery(sql)
    if (res.next) {
      val result = res.getString("id")
      System.out.println("+---+")
      System.out.println("| id|")
      System.out.println("+---+")
      System.out.println(s"| $result |")
      System.out.println("+---+")

    }
      carbon.stop()
  }

}