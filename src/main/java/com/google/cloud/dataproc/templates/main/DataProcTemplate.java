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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.PROJECT_ID_PROP;

import com.google.cloud.MonitoredResource;
import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.BaseTemplate.TemplateName;
import com.google.cloud.dataproc.templates.bigquery.BigQueryToGCS;
import com.google.cloud.dataproc.templates.databases.SpannerToGCS;
import com.google.cloud.dataproc.templates.gcs.GCSToSpanner;
import com.google.cloud.dataproc.templates.gcs.GCStoBigquery;
import com.google.cloud.dataproc.templates.general.GeneralTemplate;
import com.google.cloud.dataproc.templates.hive.HiveToBigQuery;
import com.google.cloud.dataproc.templates.hive.HiveToGCS;
import com.google.cloud.dataproc.templates.pubsub.PubSubToBQ;
import com.google.cloud.dataproc.templates.s3.S3ToBigQuery;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.word.WordCount;
import com.google.cloud.logging.*;
import com.google.cloud.spark.bigquery.repackaged.com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.cloud.spark.bigquery.repackaged.com.google.api.gax.rpc.HeaderProvider;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.BigQuery;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.BigQueryOptions;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataProcTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataProcTemplate.class);

  static final Map<TemplateName, Function<String[], BaseTemplate>> TEMPLATE_FACTORIES =
      ImmutableMap.<TemplateName, Function<String[], BaseTemplate>>builder()
          .put(TemplateName.WORDCOUNT, (args) -> new WordCount())
          .put(TemplateName.HIVETOGCS, (args) -> new HiveToGCS())
          .put(TemplateName.HIVETOBIGQUERY, (args) -> new HiveToBigQuery())
          .put(TemplateName.PUBSUBTOBQ, (args) -> new PubSubToBQ())
          .put(TemplateName.GCSTOBIGQUERY, (args) -> new GCStoBigquery())
          .put(TemplateName.BIGQUERYTOGCS, (args) -> new BigQueryToGCS())
          .put(TemplateName.S3TOBIGQUERY, (args) -> new S3ToBigQuery())
          .put(TemplateName.SPANNERTOGCS, (args) -> new SpannerToGCS())
          .put(TemplateName.GCSTOSPANNER, GCSToSpanner::of)
          .put(TemplateName.GENERAL, GeneralTemplate::of)
          .build();
  private static final String TEMPLATE_NAME_LONG_OPT = "template";
  private static final String TEMPLATE_PROPERTY_LONG_OPT = "templateProperty";

  private static final Option TEMPLATE_OPTION =
      OptionBuilder.withLongOpt(TEMPLATE_NAME_LONG_OPT)
          .hasArgs(1)
          .isRequired(true)
          .withDescription("the name of the template to run")
          .create();
  private static final Option PROPERTY_OPTION =
      OptionBuilder.withValueSeparator()
          .hasArgs(2)
          .withArgName("property=value")
          .withLongOpt(TEMPLATE_PROPERTY_LONG_OPT)
          .withDescription("Value for given property")
          .create();
  private static final Options options =
      new Options().addOption(TEMPLATE_OPTION).addOption(PROPERTY_OPTION);

  /**
   * Parse command line arguments
   *
   * @param args command line arguments
   * @return parsed arguments
   */
  public static CommandLine parseArguments(String... args) {
    CommandLineParser parser = new BasicParser();
    LOGGER.info("Parsing arguments {}", (Object) args);
    try {
      return parser.parse(options, args, true);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }

  /**
   * Parse template name enum from template name string
   *
   * @param templateNameString template name cli argument
   */
  public static TemplateName parseTemplateName(String templateNameString) {
    try {
      return TemplateName.valueOf(templateNameString.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Unexpected template name: %s", templateNameString), e);
    }
  }

  public static void main(String... args) {
    BaseTemplate template = createTemplateAndRegisterProperties(args);
    runSparkJob(template);
  }

  private static void printHelp() {
    String header = "Execute dataproc templates\n\n";
    String footer =
        "\nPlease report issues at https://github.com/GoogleCloudPlatform/dataproc-templates";
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("<spark submit> -- ", header, options, footer, true);
  }

  /**
   * Get template factory and construct template from command line args and registers any properties
   * passed on the command line.
   *
   * @param args Command line args
   * @return the constructed template
   */
  static BaseTemplate createTemplateAndRegisterProperties(String... args) {
    TemplateName templateName;
    String[] remainingArgs;
    try {
      CommandLine cmd = parseArguments(args);
      templateName = parseTemplateName(cmd.getOptionValue(TEMPLATE_NAME_LONG_OPT));
      Properties properties = cmd.getOptionProperties(TEMPLATE_PROPERTY_LONG_OPT);
      remainingArgs = cmd.getArgs();
      LOGGER.info("Template name: {}", templateName);
      LOGGER.info("Properties: {}", properties);
      LOGGER.info("Remaining args: {}", (Object) remainingArgs);
      PropertyUtil.registerProperties(properties);
    } catch (IllegalArgumentException e) {
      LOGGER.error(e.getMessage(), e);
      printHelp();
      throw e;
    }

    trackTemplateInvocation(templateName);

    if (TEMPLATE_FACTORIES.containsKey(templateName)) {
      return TEMPLATE_FACTORIES.get(templateName).apply(remainingArgs);
    } else {
      throw new IllegalArgumentException(
          String.format("Unexpected template name: %s", templateName));
    }
  }

  private static void trackTemplateInvocation(TemplateName templateName) {
    trackUsingBQ(templateName);
    trackUsingBQWithTemplateName(templateName);
    trackUsingLogging(templateName);
    trackUsingLoggingWithName(templateName);
  }

  private static void trackUsingBQ(TemplateName templateName) {
    try {
      String USER_AGENT_HEADER = "user-agent";
      String USER_AGENT_VALUE = "google-pso-tool/dataproc-templates/0.1.0";

      HeaderProvider headerProvider =
          FixedHeaderProvider.create(ImmutableMap.of(USER_AGENT_HEADER, USER_AGENT_VALUE));

      String projectId = PropertyUtil.getProperties().getProperty(PROJECT_ID_PROP);

      BigQuery bigquery =
          BigQueryOptions.newBuilder()
              .setProjectId(projectId)
              .setHeaderProvider(headerProvider)
              .build()
              .getService();
      bigquery.listDatasets();
    } catch (Exception e) {
      // log error message and ignore
      LOGGER.info("Unable to list datasets. Error:" + e.getMessage());
    }
  }

  private static void trackUsingBQWithTemplateName(TemplateName templateName) {
    try {
      String USER_AGENT_HEADER = "user-agent";
      String USER_AGENT_VALUE = "google-pso-tool/dataproc-templates/0.1.0-" + templateName;

      HeaderProvider headerProvider =
          FixedHeaderProvider.create(ImmutableMap.of(USER_AGENT_HEADER, USER_AGENT_VALUE));

      String projectId = PropertyUtil.getProperties().getProperty(PROJECT_ID_PROP);

      BigQuery bigquery =
          BigQueryOptions.newBuilder()
              .setProjectId(projectId)
              .setHeaderProvider(headerProvider)
              .build()
              .getService();
      bigquery.listDatasets();
    } catch (Exception e) {
      // log error message and ignore
      LOGGER.info("Unable to list datasets. Error:" + e.getMessage());
    }
  }

  private static void trackUsingLogging(TemplateName templateName) {
    try {
      String USER_AGENT_HEADER = "user-agent";
      String USER_AGENT_VALUE = "google-pso-tool/dataproc-templates/0.1.0";

      com.google.api.gax.rpc.HeaderProvider headerProvider =
          com.google.api.gax.rpc.FixedHeaderProvider.create(
              ImmutableMap.of(USER_AGENT_HEADER, USER_AGENT_VALUE));

      String projectId = PropertyUtil.getProperties().getProperty(PROJECT_ID_PROP);

      Logging logging =
          LoggingOptions.newBuilder()
              .setProjectId(projectId)
              .setHeaderProvider(headerProvider)
              .build()
              .getService();

      String payload = "Invkoking the template dataproc serverless template: " + templateName;
      LogEntry entry =
          LogEntry.newBuilder(Payload.StringPayload.of(payload))
              .setSeverity(Severity.INFO)
              .setLogName("dataproc-templates")
              .setResource(MonitoredResource.newBuilder("global").build())
              .build();
      logging.write(Collections.singleton(entry));
    } catch (Exception e) {
      // log error message and ignore
      LOGGER.info("Unable to list datasets. Error:" + e.getMessage());
    }
  }

  private static void trackUsingLoggingWithName(TemplateName templateName) {
    try {
      String USER_AGENT_HEADER = "user-agent";
      String USER_AGENT_VALUE = "google-pso-tool/dataproc-templates/0.1.0-" + templateName;

      com.google.api.gax.rpc.HeaderProvider headerProvider =
          com.google.api.gax.rpc.FixedHeaderProvider.create(
              ImmutableMap.of(USER_AGENT_HEADER, USER_AGENT_VALUE));

      String projectId = PropertyUtil.getProperties().getProperty(PROJECT_ID_PROP);

      Logging logging =
          LoggingOptions.newBuilder()
              .setProjectId(projectId)
              .setHeaderProvider(headerProvider)
              .build()
              .getService();

      String payload = "Invkoking the template dataproc serverless template: " + templateName;
      LogEntry entry =
          LogEntry.newBuilder(Payload.StringPayload.of(payload))
              .setSeverity(Severity.INFO)
              .setLogName("dataproc-templates")
              .setResource(MonitoredResource.newBuilder("global").build())
              .build();
      logging.write(Collections.singleton(entry));
    } catch (Exception e) {
      // log error message and ignore
      LOGGER.info("Unable to list datasets. Error:" + e.getMessage());
    }
  }

  /**
   * Run spark job for template.
   *
   * @param template the template to run.
   */
  static void runSparkJob(BaseTemplate template) {
    LOGGER.debug("Start runSparkJob");
    template.runTemplate();
    LOGGER.debug("End runSparkJob");
  }
}
