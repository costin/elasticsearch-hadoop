/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.spark.integration.ml;

import java.{lang => jl}
import java.sql.Timestamp
import java.{util => ju}
import java.util.concurrent.TimeUnit
import scala.beans.BeanInfo
import scala.collection.JavaConversions.propertiesAsScalaMap
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.SQLContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.storage.StorageLevel._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.hadoop.mr.RestUtils
import org.elasticsearch.hadoop.util.StringUtils
import org.elasticsearch.hadoop.util.TestSettings
import org.elasticsearch.hadoop.util.TestUtils
import org.elasticsearch.spark._
import org.elasticsearch.spark.cfg._
import org.elasticsearch.spark.integration.SparkUtils
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL
import org.elasticsearch.spark.sql.sqlContextFunctions
import org.hamcrest.Matchers.containsString
import org.hamcrest.Matchers.is
import org.hamcrest.Matchers.not
import org.junit.AfterClass
import org.junit.Assert._
import org.junit.Assume._
import org.junit.BeforeClass
import org.junit.FixMethodOrder
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.junit.runners.MethodSorters
import com.esotericsoftware.kryo.io.{Input => KryoInput}
import com.esotericsoftware.kryo.io.{Output => KryoOutput}
import javax.xml.bind.DatatypeConverter
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import org.elasticsearch.hadoop.serialization.EsHadoopSerializationException
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.mllib.linalg.Vector
import org.elasticsearch.spark.ml.feature.ESAnalyzer
import org.elasticsearch.spark.ml.rest.MLRestClient
import org.elasticsearch.spark.sql._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors

object AbstractScalaEsScalaSparkML {
  @transient val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .setMaster("local").setAppName("estest")
    .set("spark.executor.extraJavaOptions", "-XX:MaxPermSize=256m")
    .set("spark.executor.memory", "512m")
    .setJars(SparkUtils.ES_SPARK_TESTING_JAR)
  @transient var cfg: SparkConf = null
  @transient var sc: SparkContext = null
  @transient var sqc: SQLContext = null

  @BeforeClass
  def setup() {
    conf.setAll(TestSettings.TESTING_PROPS);
    sc = new SparkContext(conf)
    sqc = new SQLContext(sc)
  }

  @AfterClass
  def cleanup() {
    if (sc != null) {
      sc.stop
      // give jetty time to clean its act up
      Thread.sleep(TimeUnit.SECONDS.toMillis(3))
    }
  }

  @Parameters
  def testParams(): ju.Collection[Array[jl.Object]] = {
    val list = new ju.ArrayList[Array[jl.Object]]()
    // no query
    val noQuery = ""
    list.add(Array("default", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.TRUE, noQuery))

    // uri query
    val uriQuery = "?q=*"
    list.add(Array("defaulturiquery", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.TRUE, uriQuery))
    
    // dsl query
    val dslQuery = """ {"query" : { "match_all" : { } } } """
    list.add(Array("defaultdslquery", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.TRUE, dslQuery))

    list
  }
}

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(classOf[Parameterized])
class AbstractScalaEsScalaSparkML(prefix: String, readMetadata: jl.Boolean, pushDown: jl.Boolean, strictPushDown: jl.Boolean, doubleFiltering: jl.Boolean, query: String = "") extends Serializable {

  val sc = AbstractScalaEsScalaSparkML.sc
  val sqc = AbstractScalaEsScalaSparkML.sqc
  val cfg = Map(ES_QUERY -> query,
                ES_READ_METADATA -> readMetadata.toString(),
                "es.internal.spark.sql.pushdown" -> pushDown.toString(),
                "es.internal.spark.sql.pushdown.strict" -> strictPushDown.toString(),
                "es.internal.spark.sql.pushdown.keep.handled.filters" -> doubleFiltering.toString())

  val datInput = TestUtils.sampleArtistsDat()

  import sqc.implicits._

  def exampleTrainingData = {
     sqc.createDataFrame(Seq(
        (0L, "a b c d e spark", 1.0),
        (1L, "b d", 0.0),
        (2L, "spark f g h", 1.0),
        (3L, "hadoop mapreduce", 0.0)
     )).toDF("id", "text", "label")
  }
  
  def exampleTestingData = {
     sqc.createDataFrame(Seq(
       (4L, "spark i j k"),
       (5L, "l m n"),
       (6L, "mapreduce spark"),
       (7L, "apache hadoop")
     )).toDF("id", "text")
  }
  
  //@Test
  def testTransformAnalyzer() {
    val training = exampleTrainingData 

    val analyzer = new ESAnalyzer()(sqc)
      .setInputCol("text")
      .setOutputCol("words")

    analyzer.transform(training.toDF())
    
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(analyzer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)      
    val pipeline = new Pipeline()
      .setStages(Array(analyzer, hashingTF, lr))      

    // Fit the pipeline to training documents.
    val model = pipeline.fit(training.toDF())
    
    // Prepare test documents, which are unlabeled.
    val test = exampleTestingData

    // Make predictions on test documents.
    model.transform(test.toDF())
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }
  }


  @Test
  def testVectorizeApi() {
    // this test assumes the sample data (Twitter/What not) is already loaded
    val specId = wrapIndex("all_terms-test")
    val length = prepareSpecAllTerms(specId, "text", "movie-reviews", 3).get("length").asInstanceOf[Int]
    
    val payload = s"""{
	  | "script_fields" : {
		|   "vector" : {
		|      "script" : {
		|        "id" : "${specId}",
		|        "lang" : "pmml_vector"
		|       }
		|      }
	  |    }
    |}
    |""".stripMargin

    // can't use DF since that one relies on the mapping and wrapping afterwards does not make sense
    val rawData = sc.esRDD("movie-reviews/review", payload, Map(ConfigurationOptions.ES_READ_UNMAPPED_FIELDS_IGNORE -> "false"))
    
    val corpus = createCorpus(rawData, length)
    val split = corpus.randomSplit(Array(0.9, 0.1), seed = 12345)
    
    val trainingData = split(0)
    val testData = split(1)
    
    // maybe add a ParamGridBuilder example?
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
   
    val model = lr.fit(trainingData)
    
    model.transform(testData)
         .select("features", "label", "prediction")
         .show(10)
  }

  def createCorpus(rdd: RDD[(String, Map[String, AnyRef])], length: Int) = {
    // on a DF it would be
    // similar to when(x("label") === "negative", 0.0d).otherwise(1.0d)

    rdd.map( x => {
       val label = if (x._1 == "negative") 0.0d else 1.0d
       // TODO: Britta - why is there a list returned?
       val vectorData = x._2("vector").asInstanceOf[Seq[AnyRef]](0).asInstanceOf[Map[String, AnyRef]]
       // extract the data
       val indices = vectorData.getOrElse("indices", List.empty[Int]).asInstanceOf[Seq[Int]].toArray
       val values = vectorData.getOrElse("values", List.empty[Double]).asInstanceOf[Seq[Double]].toArray
       // pack it into a vector
       // to avoid some overhead, pass the data as arrays instead of creating the Seq of tuples (indices.zip(values)) since they would be unpacked anyway
       val vector = Vectors.sparse(length, indices, values)
       
       (label, vector)
     }
    ).toDF("label", "features")
  }
  
  def prepareSpecAllTerms(specId: String, field: String, index: String, minDocFreq: Int) = {
    val payload = s"""{ "features" : [{
        | "field": "${field}",
        | "type" : "string",
        | "tokens" : "all_terms",
        | "number" : "occurrence",
        | "index"  : "${index}",
        | "min_doc_freq" : ${minDocFreq}
        | }], "sparse" : "true"}""".stripMargin.getBytes(StringUtils.UTF_8)
    
    RestUtils.postData("/_prepare_spec?id=" + specId, payload) 
  }
  
  
  //@Test
  def testExampleTextClassificationPipeline() {
    
    val training = exampleTrainingData
    
    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    // Fit the pipeline to training documents.
    val model = pipeline.fit(training)

    val test = exampleTestingData
    
    // Make predictions on test documents.
    model.transform(test)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }
  }

  def wrapIndex(index: String) = {
    prefix + index
  }
}