package com.google.cloud.dataproc.templates.redshift;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.storage.*;

class RedshiftToGCSTest {

    private RedshiftToGCS RedshiftToGCSTest;
    private static final Logger LOGGER = LoggerFactory.getLogger(RedshiftToGCSTest.class);

    @BeforeEach
    void setUp() {
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
    }

    @ParameterizedTest
    @MethodSource("propertyKeys")
    void runTemplateWithValidParameters(String propKey) {
        LOGGER.info("Running test: runTemplateWithValidParameters");
        PropertyUtil.getProperties().setProperty(REDSHIFT_GCS_INPUT_URL, "someValue");
        PropertyUtil.getProperties().setProperty(REDSHIFT_GCS_INPUT_TABLE, "someValue");
        PropertyUtil.getProperties().setProperty(REDSHIFT_GCS_TEMP_DIR, "someValue");
        PropertyUtil.getProperties().setProperty(REDSHIFT_GCS_IAM_ROLE, "someValue");
        PropertyUtil.getProperties().setProperty(REDSHIFT_GCS_ACCESS_KEY, "someValue");
        PropertyUtil.getProperties().setProperty(REDSHIFT_GCS_SECRET_KEY, "someValue");
        PropertyUtil.getProperties().setProperty(REDSHIFT_GCS_FILE_FORMAT, "someValue");
        PropertyUtil.getProperties().setProperty(REDSHIFT_GCS_FILE_LOCATION, "someValue");
        PropertyUtil.getProperties().setProperty(propKey, "someValue");
        RedshiftToGCSTest = new RedshiftToGCS();

        assertDoesNotThrow(RedshiftToGCSTest::validateInput);
    }

    @ParameterizedTest
    @MethodSource("propertyKeys")
    void runTemplateWithInvalidParameters(String propKey) {
        LOGGER.info("Running test: runTemplateWithInvalidParameters");
        PropertyUtil.getProperties().setProperty(propKey, "");
        RedshiftToGCSTest = new RedshiftToGCS();

        Exception exception =
                assertThrows(IllegalArgumentException.class, () -> RedshiftToGCSTest.runTemplate());
        assertEquals(
                "Required parameters for RedshiftToGCS not passed. "
                        + "Set mandatory parameter for RedshiftToGCS template in "
                        + "resources/conf/template.properties file.",
                exception.getMessage());
    }

    static Stream<String> propertyKeys() {
        return Stream.of(
                REDSHIFT_GCS_INPUT_URL,
                REDSHIFT_GCS_INPUT_TABLE,
                REDSHIFT_GCS_TEMP_DIR,
                REDSHIFT_GCS_IAM_ROLE,
                REDSHIFT_GCS_ACCESS_KEY,
                REDSHIFT_GCS_SECRET_KEY,
                REDSHIFT_GCS_FILE_FORMAT,
                REDSHIFT_GCS_FILE_LOCATION);
    }
}
