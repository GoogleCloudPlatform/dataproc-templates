package com.google.cloud.dataproc.templates.databases;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.cloud.dataproc.templates.util.PropertyUtil;

import java.util.stream.Stream;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;
import static org.junit.jupiter.api.Assertions.*;

public class CassandraToGCSTest {
    private CassandraToGCS cassandraToGCS;
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraToGCSTest.class);

    @BeforeEach
    void setUp() {
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
    }

    @ParameterizedTest
    @MethodSource("propertyKeys")
    void runTemplateWithValidParameters(String propKey) {
        LOGGER.info("Running test: runTemplateWithValidParameters");
        PropertyUtil.getProperties().setProperty(CASSANDRA_TO_GSC_OUTPUT_PATH, "gs://test-bucket/output");
        PropertyUtil.getProperties()
                .setProperty(
                        CASSANDRA_TO_GSC_OUTPUT_SAVE_MODE, "append");
        PropertyUtil.getProperties().setProperty(CASSANDRA_TO_GSC_OUTPUT_FORMAT, "csv");
        PropertyUtil.getProperties().setProperty(CASSANDRA_TO_GSC_INPUT_HOST, "host");
        PropertyUtil.getProperties().setProperty(CASSANDRA_TO_GSC_INPUT_TABLE, "table");
        PropertyUtil.getProperties().setProperty(CASSANDRA_TO_GSC_INPUT_KEYSPACE, "keyspace");
        cassandraToGCS = new CassandraToGCS();
        assertDoesNotThrow(cassandraToGCS::validateInput);
    }

    @ParameterizedTest
    @MethodSource("propertyKeys")
    void runTemplateWithInvalidParameters(String propKey) {
        LOGGER.info("Running test: runTemplateWithInvalidParameters");
        PropertyUtil.getProperties().setProperty(propKey, "");
        cassandraToGCS = new CassandraToGCS();

        Exception exception =
                assertThrows(IllegalArgumentException.class, () -> cassandraToGCS.runTemplate());
        assertEquals(
                "Required parameters for CassandraToGCS not passed. "
                        + "Set mandatory parameter for CassandraToGCS template in "
                        + "resources/conf/template.properties file.",
                exception.getMessage());
    }

    static Stream<String> propertyKeys() {
        return Stream.of(
                CASSANDRA_TO_GSC_OUTPUT_PATH,
                CASSANDRA_TO_GSC_OUTPUT_SAVE_MODE,
                CASSANDRA_TO_GSC_OUTPUT_FORMAT,
                CASSANDRA_TO_GSC_INPUT_HOST,
                CASSANDRA_TO_GSC_INPUT_TABLE,
                CASSANDRA_TO_GSC_INPUT_KEYSPACE);
    }
}
