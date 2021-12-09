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
package com.google.cloud.dataproc.templates.main;

import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.BaseTemplate.TemplateName;
import com.google.cloud.dataproc.templates.general.GeneralTemplate;
import com.google.cloud.dataproc.templates.databases.SpannerToGCS;
import com.google.cloud.dataproc.templates.gcs.GCStoBigquery;
import com.google.cloud.dataproc.templates.hive.HiveToBigQuery;
import com.google.cloud.dataproc.templates.hive.HiveToGCS;
import com.google.cloud.dataproc.templates.pubsub.PubSubToBQ;
import com.google.cloud.dataproc.templates.s3.S3ToBigQuery;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.word.WordCount;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataProcTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataProcTemplate.class);
  private static final String TEMPLATE_NAME_OPT = "template";
  private static final String TEMPLATE_PROPERTY_NAME_OPT = "prop";

  /**
   * Parse command line arguments
   *
   * @param args command line arguments
   * @return parsed arguments
   */
  public static CommandLine parseArguments(String... args) {
    Options options = new Options();

    Option templateOption = new Option(TEMPLATE_NAME_OPT, "the name of the template to run");
    templateOption.setRequired(true);
    templateOption.setArgs(1);
    options.addOption(templateOption);

    @SuppressWarnings("AccessStaticViaInstance")
    Option propertyOption =
        OptionBuilder.withValueSeparator()
            .hasArgs(2)
            .withArgName("property=value")
            .withLongOpt(TEMPLATE_PROPERTY_NAME_OPT)
            .withDescription("Value for given property")
            .create();
    options.addOption(propertyOption);

    CommandLineParser parser = new BasicParser();
    LOGGER.info("Parsing arguments {}", (Object) args);
    try {
      return parser.parse(options, args, true);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }

  public static void main(String[] args) {
    CommandLine cmd = parseArguments(args);
    String templateNameString = cmd.getOptionValue(TEMPLATE_NAME_OPT);
    Properties properties = cmd.getOptionProperties(TEMPLATE_PROPERTY_NAME_OPT);
    String[] remainingArgs = cmd.getArgs();
    LOGGER.info("Template name: {}", templateNameString);
    LOGGER.info("Properties: {}", properties);
    LOGGER.info("Remaining args: {}", (Object) remainingArgs);
    PropertyUtil.registerProperties(properties);

    TemplateName templateName;
    try {
      templateName = TemplateName.valueOf(templateNameString.trim().toUpperCase());
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException(String.format("Unexpected template name: %s", templateNameString), ex);
    }
    LOGGER.info("Running job for {}", templateName);
    runSparkJob(templateName, cmd.getArgs());
  }

  static final Map<TemplateName, Function<String[], BaseTemplate>> templateFactories =
      ImmutableMap.<TemplateName, Function<String[], BaseTemplate>>builder()
          .put(TemplateName.WORDCOUNT, (args) -> new WordCount())
          .put(TemplateName.HIVETOGCS, (args) -> new HiveToGCS())
          .put(TemplateName.HIVETOBIGQUERY, (args) -> new HiveToBigQuery())
          .put(TemplateName.PUBSUBTOBQ, (args) -> new PubSubToBQ())
          .put(TemplateName.GCSTOBIGQUERY, (args) -> new GCStoBigquery())
          .put(TemplateName.S3TOBIGQUERY, (args) -> new S3ToBigQuery())
          .put(TemplateName.SPANNERTOGCS, (args) -> new SpannerToGCS())
          .put(TemplateName.GENERAL, GeneralTemplate::of)
          .build();

  /**
   * Run spark job for template.
   *
   * @param templateName name of the template to execute.
   */
  static void runSparkJob(TemplateName templateName, String[] args) {
    LOGGER.debug("Start API runSparkJob");
    BaseTemplate template;
    if (templateFactories.containsKey(templateName)) {
      template = templateFactories.get(templateName).apply(args);
    } else {
      throw new IllegalArgumentException(
          String.format("Unexpected template name: %s", templateName));
    }
    template.runTemplate();
    LOGGER.debug("End API runSparkJob");
  }
}
