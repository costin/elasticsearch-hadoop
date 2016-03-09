package org.elasticsearch.spark.integration.ml;

import org.elasticsearch.hadoop.LocalEs;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ AbstractScalaEsScalaSparkML.class })
public class SparkMLScalaSuite {

    @ClassRule
    public static ExternalResource resource = new LocalEs();
}