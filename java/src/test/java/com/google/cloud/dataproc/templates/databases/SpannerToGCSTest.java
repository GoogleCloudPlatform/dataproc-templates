package com.google.cloud.dataproc.templates.databases;


import static com.google.cloud.dataproc.templates.util.TemplateConstants.PROJECT_ID_PROP;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.SPANNER_GCS_INPUT_DATABASE_ID;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.SPANNER_GCS_INPUT_SPANNER_INSTANCE_ID;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.SPANNER_GCS_INPUT_TABLE_ID;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.SPANNER_GCS_OUTPUT_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.SPANNER_GCS_OUTPUT_GCS_PATH;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.SPANNER_GCS_OUTPUT_GCS_SAVEMODE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Properties;
import java.util.stream.Stream;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataproc.templates.databases.SpannerToGCS;
import com.google.cloud.dataproc.templates.util.PropertyUtil;

public class SpannerToGCSTest {

	private SpannerToGCS spannerToGCSTest;

	  private static final Logger LOGGER = LoggerFactory.getLogger(SpannerToGCSTest.class);

	  
	  @Test
	  void runTemplateWithValidParameters() {
	    LOGGER.info("Running test: runTemplateWithValidParameters");
	    Properties props = PropertyUtil.getProperties();
	    props.setProperty(PROJECT_ID_PROP, "testProject");
	    props.setProperty(SPANNER_GCS_INPUT_SPANNER_INSTANCE_ID, "spannerinstance");
	    props.setProperty(SPANNER_GCS_INPUT_DATABASE_ID, "spannerdatabase");
	    props.setProperty(SPANNER_GCS_INPUT_TABLE_ID, "tableid");
	    props.setProperty(SPANNER_GCS_OUTPUT_GCS_PATH, "gs://temp-bucket");
	    props.setProperty(SPANNER_GCS_OUTPUT_GCS_SAVEMODE, "append");
	    props.setProperty(SPANNER_GCS_OUTPUT_FORMAT, "cvs");
	    spannerToGCSTest = new SpannerToGCS();

	    assertDoesNotThrow(spannerToGCSTest::validateInput);
	  }

	  @ParameterizedTest
	  @MethodSource("propertyKeys")
	  void runTemplateWithInvalidParameters(String propKey) {
	    LOGGER.info("Running test: runTemplateWithInvalidParameters");
	    PropertyUtil.getProperties().setProperty(propKey, "");
	    spannerToGCSTest = new SpannerToGCS();
	    Exception exception =
	        assertThrows(IllegalArgumentException.class, () -> spannerToGCSTest.validateInput());
	    assertEquals(
	        "Required parameters for SpannerToGCS not passed. "
	            + "Set mandatory parameter for SpannerToGCS template"
	            + " in resources/conf/template.properties file or at runtime."
	            + " Refer to d/README.md for more instructions.",
	        exception.getMessage());
	  }

	  static Stream<String> propertyKeys() {
	    return Stream.of(
	    		SPANNER_GCS_INPUT_SPANNER_INSTANCE_ID, SPANNER_GCS_INPUT_DATABASE_ID, SPANNER_GCS_INPUT_TABLE_ID, SPANNER_GCS_OUTPUT_GCS_PATH);
	  }
}
