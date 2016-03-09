package org.elasticsearch.spark.ml.feature

import org.apache.spark.ml.param.Param
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.elasticsearch.spark.ml.rest.MLRestClient
import org.apache.spark.sql.Column

// private trait similar to that in Spark however
// extending it to take into account SqlContext
private[ml] trait UnaryTransformer[IN, OUT, T <: UnaryTransformer[IN, OUT, T]] extends Transformer 
  with ESFeature {
  
  // HasInputCol
  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")

  final def getInputCol: String = $(inputCol)
  
  // HasOutputCol
  final val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")

  setDefault(outputCol, uid + "__output")

  final def getOutputCol: String = $(outputCol)

  // Unary
  def setInputCol(value: String): T = set(inputCol, value).asInstanceOf[T]
  
  def setOutputCol(value: String): T = set(outputCol, value).asInstanceOf[T]

  protected def outputDataType: DataType
  
  protected def validateInputType(inputType: DataType): Unit = {}
  
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    validateInputType(inputType)
    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    val outputFields = schema.fields :+
      StructField($(outputCol), outputDataType, nullable = false)
    StructType(outputFields)
  }

  // modified Transformer
  override def transform(dataset: DataFrame): DataFrame = {
    def transformWrapper = callUDF((in:IN) => {
      val client = restClient
        try {
           esTransform(in)(client, dataset)
        } finally {
           client.close()
        }
      }, outputDataType, dataset($(inputCol)))
    
    dataset.withColumn($(outputCol), transformWrapper)
  }
  
  protected def esTransform(in: IN)(client: MLRestClient, dataset: DataFrame): OUT
}