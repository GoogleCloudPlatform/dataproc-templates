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
package com.google.cloud.dataproc.templates.general;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.config.GeneralTemplateConfig;
import com.google.cloud.dataproc.templates.config.InputConfig;
import com.google.cloud.dataproc.templates.config.OutputConfig;
import com.google.cloud.dataproc.templates.config.QueryConfig;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.arrow.util.Preconditions;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeneralTemplate implements BaseTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeneralTemplate.class);

  public static String CONFIG_FILE_OPTION = "config";
  public static String DRY_RUN_OPTION = "dryrun";

  private SparkSession spark;
  private final GeneralTemplateConfig config;

  public GeneralTemplate(GeneralTemplateConfig config) {
    this.config = config;
  }

  /**
   * Main method to parse arguments, load config and start the spark session then construct and
   * running call run on the template
   *
   * <p>If --dryrun is set, then a guard clause exits before the spark session is created.
   */
  @Override
  public void runTemplate() {

    try (SparkSession spark = SparkSession.builder().appName("Generic Template").getOrCreate()) {
      this.run(spark);
    }
  }

  @Override
  public void validateInput() {
    ValidationUtil.validateOrThrow(config);
  }

  public static GeneralTemplate of(String... args) {
    CommandLine cmd = parseArguments(args);
    Path configPath = Paths.get(cmd.getOptionValue(CONFIG_FILE_OPTION));

    GeneralTemplateConfig config = null;
    try {
      config = loadConfigYaml(configPath);
    } catch (IOException e) {
      throw new IllegalArgumentException("Could not load config yaml", e);
    }
    LOGGER.info("Config loaded\n{}", config);
    return new GeneralTemplate(config);
  }

  /**
   * Parse command line arguments to supply template configuration yaml file at startup
   *
   * @param args command line arguments
   * @return parsed arguments
   */
  public static CommandLine parseArguments(String... args) {
    Options options = new Options();
    Option configFileOption =
        new Option(
            CONFIG_FILE_OPTION, "yaml file containing template configuration, can be a gcs path");
    configFileOption.setRequired(true);
    configFileOption.setArgs(1);
    options.addOption(configFileOption);

    Option dryRunOption =
        new Option(DRY_RUN_OPTION, false, "load configuration but do not execute spark template");
    options.addOption(dryRunOption);

    CommandLineParser parser = new BasicParser();
    LOGGER.info("Parsing arguments {}", (Object) args);
    try {
      return parser.parse(options, args);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Fetch and parse template configuration yaml file
   *
   * @param path the template configuration yaml file path
   * @return a configuration containing settings for the generic template
   * @throws IOException when config cannot be loaded from disk or yaml cannot be mapped to {@link
   *     GeneralTemplateConfig}
   */
  public static GeneralTemplateConfig loadConfigYaml(Path path) throws IOException {
    LOGGER.info("Loading config from {}", path.toUri());
    byte[] bytes = Files.readAllBytes(path);
    LOGGER.info("File fetched");
    LOGGER.info("Parsing yaml");
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    mapper.findAndRegisterModules();
    return mapper.readValue(bytes, GeneralTemplateConfig.class);
  }

  /**
   * Read a dataset
   *
   * @param inputConfig configuration for reading the input
   * @return loaded dataset
   */
  public Dataset<Row> read(InputConfig inputConfig) {
    DataFrameReader reader =
        spark.read().format(inputConfig.getFormat()).options(inputConfig.getOptions());
    String path = inputConfig.getPath();
    return inputConfig.getPath() != null ? reader.load(path) : reader.load();
  }

  /**
   * Write a dataset
   *
   * @param dataset dataset to be written
   * @param outputConfig configuration for writing the output
   */
  public void write(Dataset<Row> dataset, OutputConfig outputConfig) {
    DataFrameWriter<?> writer =
        dataset.write().format(outputConfig.getFormat()).options(outputConfig.getOptions());
    if (outputConfig.getMode() != null) {
      writer = writer.mode(outputConfig.getMode());
    }
    if (outputConfig.getPath() != null) {
      writer.save(outputConfig.getPath());
    } else {
      writer.save();
    }
  }

  /**
   * Run the template by reading all inputs, applying any queries to those inputs, and then writing
   * all outputs.
   *
   * <p>The keys for inputs, queries and outputs in the template configuration correspond to
   * temporary spark sql views. So the queries can query over any input as if it's a table, and the
   * output's key must match an input or query that we wish to write.
   */
  public void run(SparkSession spark) {
    this.spark = spark;
    Map<String, InputConfig> inputConfig = config.getInput();
    for (Entry<String, InputConfig> entry : inputConfig.entrySet()) {
      LOGGER.info("Loading input table {}", entry.getKey());
      Dataset<Row> dataset = read(entry.getValue());
      dataset.createOrReplaceTempView(entry.getKey());
    }

    for (Entry<String, QueryConfig> entry : config.getQuery().entrySet()) {
      String sql = entry.getValue().getSql();
      LOGGER.info("Executing query {}", sql);
      Dataset<Row> dataset = spark.sql(sql);
      dataset.createOrReplaceTempView(entry.getKey());
    }

    for (Entry<String, OutputConfig> entry : config.getOutput().entrySet()) {
      LOGGER.info("Writing output table {}", entry.getKey());
      Dataset<Row> dataset = spark.table(entry.getKey());
      Preconditions.checkNotNull(
          dataset, String.format("No matching table for name %s, ", entry.getKey()));
      write(dataset, entry.getValue());
    }
  }
}
