package org.elasticsearch.spark.ml.feature

import org.apache.spark.ml.util.DefaultParamsWritable
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.param.ParamValidators
import org.apache.spark.ml.param.shared.HasOutputCol
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.ml.param.IntParam
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.DefaultParamsWritable
import org.apache.spark.annotation.Since
import org.apache.spark.ml.util.DefaultParamsReadable
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StringType
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.param.StringArrayParam
import org.elasticsearch.hadoop.util.StringUtils._
import org.elasticsearch.hadoop.rest.RestClient
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.ml.rest.MLRestClient
import org.elasticsearch.hadoop.util.StringUtils._
import org.elasticsearch.hadoop.util.ObjectUtils._
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import scala.collection.JavaConverters._

@Experimental
class ESAnalyzer(override val uid: String = Identifiable.randomUID("ESAnalyzer"),
                 override val esCfg: scala.collection.Map[String, String] = Map.empty)
                 (implicit val sqlContext: SQLContext)
  extends UnaryTransformer[String, Seq[_], ESAnalyzer] 
 {
  
  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  // same output format as Tokenizer (though we can add the position as offset)
  override protected def outputDataType: DataType = new ArrayType(StringType, true)

  override def copy(extra: ParamMap): ESAnalyzer = defaultCopy(extra)
  
  val analyzer: Param[String] = new Param(this, "analyzer", "Elasticsearch analyzer");
  
  val tokenizer: Param[String] = new Param(this, "tokenizer", "Elasticsearch tokenizer");
  
  val tokenFilters: StringArrayParam = new StringArrayParam(this, "token filters", "Elasticsearch token filters");
  
  val charFilters: StringArrayParam = new StringArrayParam(this, "char filters", "Elasticsearch char filters");

  val index: Param[String] = new Param(this, "index", "Elasticsearch index");

  def setAnalyzer(value: String): this.type = set(analyzer, value)

  def getAnalyzer: String = $(analyzer)

  def setTokenizer(value: String): this.type = set(tokenizer, value)

  def getTokenizer: String = $(tokenizer)

  def setTokenFilters(value: Array[String]): this.type = set(tokenFilters, value)
  
  def getTokenFilters: Array[String] = $(tokenFilters)

  def setCharFilters(value: Array[String]): this.type = set(charFilters, value)
  
  def getCharFilters: Array[String] = $(charFilters)

  def setIndex(value: String): this.type = set(index, value)

  def getIndex: String = $(index)
  
  setDefault(analyzer -> "standard", tokenizer -> EMPTY, tokenFilters -> EMPTY_ARRAY, charFilters -> EMPTY_ARRAY, index -> EMPTY)
 
  override def esTransform(in: String)(client: MLRestClient, dataset: DataFrame): Seq[_] = {
    val url = if (hasText($(index))) s""" ${($(index))}/_analyze""" else "_analyze"
    return client.analyze(url, requestBody(in)).asScala
  }
  
  private def requestBody(in: String) = {
    val body = new StringBuilder(s"""{ "text": ${toJsonString(in)}\n""")
    if (hasText($(analyzer))) {
      body.append(s""","analyzer": "${$(analyzer)}" """)
    }
    if (hasText($(tokenizer))) {
      body.append(s""","tokenizer": "${$(tokenizer)}" """)
    }
    if ($(tokenFilters) != null && !$(tokenFilters).isEmpty) {
      body.append(s""","filters": ["${tokenFilters.jsonEncode($(tokenFilters))}"] """)
    }
    if ($(charFilters) != null && !$(charFilters).isEmpty) {
      body.append(s""","char_filters": ["${charFilters.jsonEncode($(charFilters))}"] """)
    }
    body.append("}")
    body.toString
  }
}