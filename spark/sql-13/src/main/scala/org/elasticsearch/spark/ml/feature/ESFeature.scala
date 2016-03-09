package org.elasticsearch.spark.ml.feature

import scala.collection.JavaConversions.mapAsJavaMap
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.cfg.SparkSettingsManager
import org.elasticsearch.spark.ml.rest.MLRestClient
import org.apache.spark.SparkContext

trait ESFeature {
   val esCfg: scala.collection.Map[String, String]
 
   @transient private[ml] lazy val settings = {
    val fullCfg = new SparkSettingsManager().load(SparkContext.getOrCreate().getConf).copy()
    fullCfg.merge(esCfg)
   }
   
   protected def restClient: MLRestClient = {
     new MLRestClient(settings)
   }
}