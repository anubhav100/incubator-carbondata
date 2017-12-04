package org.apache.carbondata.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, BoundReference, Expression, Literal}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType}

import scala.reflect.internal.util.TableDef.Column

object CustomAddExample extends App{

  case class CustomAddExpression(left:Expression,right:Expression) extends BinaryExpression with CodegenFallback{
    override def dataType: DataType = DoubleType

    override def eval(input: InternalRow): Any = {
      val leftValue = left.eval(input).asInstanceOf[Double]
      val rightValue = right.eval(input).asInstanceOf[Double]

      leftValue+rightValue
    }
  }
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("CarbonSessionExample")
    .getOrCreate()

  val rdd = spark.sqlContext.sparkContext.makeRDD(List(("hi",5.0),("hell0",7.0)))

  val dataFrame = spark.sqlContext.createDataFrame(rdd)

  val customAddExpression = new CustomAddExpression(BoundReference(1,DoubleType,true),Literal(2.0))

  dataFrame.explain(true)


}
