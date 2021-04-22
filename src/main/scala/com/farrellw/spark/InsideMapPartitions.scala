package com.farrellw.spark

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object InsideMapPartitions {
  lazy val logger: Logger = Logger.getLogger(this.getClass)

  implicit def stringToBytes(str: String): Array[Byte] = Bytes.toBytes(str)

  implicit def bytesToString(bytes: Array[Byte]): String = Bytes.toString(bytes)

  val jobName = "UsingMapPartitions"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(jobName).config("spark.sql.shuffle.partitions", "3").master("local[*]").getOrCreate()
    import spark.implicits._

    // Have a dataset of strings. Each string is a customer id
    val customerIds = spark.sparkContext.parallelize(
      List[String](
        "31703337",
        "28352063",
        "48382003",
        "1879452",
        "1483620"
      )).toDS()

    /*
      Initialize the connection on the executor.
      Re-use the connection on each partition to send a get request to hbase for each id
    */
    val customers = customerIds.mapPartitions(partition => {

      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "cdh01.hourswith.expert:2181,cdh02.hourswith.expert:2181,cdh03.hourswith.expert:2181")
      val connection = ConnectionFactory.createConnection(conf)
      val table = connection.getTable(TableName.valueOf("wfarrell:users"))

      val iter = partition.map(id => {
        val get = new Get(id).addFamily("f1")
        val result = table.get(get)

        (id, Bytes.toString(result.getValue("f1", "mail")))
      }).toList.iterator

      connection.close()

      iter
    })

    customers.foreach(c => println(c))
  }
}
