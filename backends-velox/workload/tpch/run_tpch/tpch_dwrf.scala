import org.apache.spark.sql.execution.debug._
import scala.io.Source
import java.io.File
import java.util.Arrays
import sys.process._


def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000.0 + " seconds")
    result
}

//Read TPC-H Table from DWRF files
val lineitem = spark.read.format("dwrf").load("file:///PATH/TO/TPCH_DWRF_PATH/lineitem")
val part = spark.read.format("dwrf").load("file:///PATH/TO/TPCH_DWRF_PATH/part")
val orders = spark.read.format("dwrf").load("file:///PATH/TO/TPCH_DWRF_PATH/orders")
val customer = spark.read.format("dwrf").load("file:///PATH/TO/TPCH_DWRF_PATH/customer")
val supplier = spark.read.format("dwrf").load("file:///PATH/TO/TPCH_DWRF_PATH/supplier")
val partsupp = spark.read.format("dwrf").load("file:///PATH/TO/TPCH_DWRF_PATH/partsupp")
val region = spark.read.format("dwrf").load("file:///PATH/TO/TPCH_DWRF_PATH/region")
val nation = spark.read.format("dwrf").load("file:///PATH/TO/TPCH_DWRF_PATH/nation")

//Create DWRF based TPC-H Table View
lineitem.createOrReplaceTempView("lineitem")
orders.createOrReplaceTempView("orders")
customer.createOrReplaceTempView("customer")
part.createOrReplaceTempView("part")
supplier.createOrReplaceTempView("supplier")
partsupp.createOrReplaceTempView("partsupp")
nation.createOrReplaceTempView("nation")
region.createOrReplaceTempView("region")


def getListOfFiles(dir: String):List[File] = {
     val d = new File(dir)
     if (d.exists && d.isDirectory) {
         //You can run a specific query by using below line
         //d.listFiles.filter(_.isFile).filter(_.getName().contains("17.sql")).toList
         d.listFiles.filter(_.isFile).toList
     } else {
         List[File]()
     }
}
val fileLists = getListOfFiles("/PATH/TO/TPCH_QUERIES/")
val sorted = fileLists.sortBy {
       f => f.getName match {
       case name =>
         var str = name
         str = str.replaceFirst("a", ".1")
         str = str.replaceFirst("b", ".2")
         str = str.replaceFirst(".sql", "")
         str = str.replaceFirst("q", "")
         str.toDouble
     }}

// Main program to run TPC-H testing
for (t <- sorted) {
  println(t)
  val fileContents = Source.fromFile(t).getLines.filter(!_.startsWith("--")).mkString(" ")
  println(fileContents)
  try {
    time{spark.sql(fileContents).show}
    //spark.sql(fileContents).explain
    Thread.sleep(2000)
  } catch {
    case e: Exception => None
  }
}
