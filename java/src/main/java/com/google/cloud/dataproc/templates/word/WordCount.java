/*
 * Copyright (C) 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataproc.templates.word;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.WORD_COUNT_INPUT_PATH_PROP;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.WORD_COUNT_OUTPUT_PATH_PROP;

import com.google.cloud.dataproc.templates.BaseTemplate;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class WordCount implements BaseTemplate {
  private static final Logger LOGGER = LoggerFactory.getLogger(WordCount.class);

  @Override
  public void runTemplate() {
    String inputPath = getProperties().getProperty(WORD_COUNT_INPUT_PATH_PROP);
    String outputPath = getProperties().getProperty(WORD_COUNT_OUTPUT_PATH_PROP);

    LOGGER.info("Starting word count spark job, inputPath:{},outputPath:{}", inputPath, outputPath);

    JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("Word Count"));
    JavaRDD<String> lines = sparkContext.textFile(inputPath);
    JavaRDD<String> words =
        lines.flatMap((String line) -> Arrays.asList(line.split(" ")).iterator());
    JavaPairRDD<String, Integer> wordCounts =
        words
            .mapToPair((String word) -> new Tuple2<>(word, 1))
            .reduceByKey((Integer count1, Integer count2) -> count1 + count2);
    wordCounts.saveAsTextFile(outputPath);
  }

  public void validateInput() {
    LOGGER.info(
        "No validation has been specified for this class, hence the method contains only a logger statement");
  }
}
