package com.google.cloud.dataproc.templates.hbase;
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

public class HbaseToGCSTest {
    private HbaseToGCS hbaseToGCSTest;
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseToGCSTest.class);
    @BeforeEach
    void setUp() {
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
    }

    @ParameterizedTest
    @MethodSource("propertyKeys")
    void runTemplateWithValidParameters(String propKey) {
        LOGGER.info("Running test: runTemplateWithValidParameters");
        PropertyUtil.getProperties().setProperty(HBASE_TO_GCS_OUTPUT_PATH, "gs://test-bucket");
        PropertyUtil.getProperties().setProperty(propKey, "someValue");
        hbaseToGCSTest = new HbaseToGCS();

        assertDoesNotThrow(hbaseToGCSTest::runTemplate);
    }

    @ParameterizedTest
    @MethodSource("propertyKeys")
    void runTemplateWithInvalidParameters(String propKey) {
        LOGGER.info("Running test: runTemplateWithInvalidParameters");
        PropertyUtil.getProperties().setProperty(propKey, "");
        hbaseToGCSTest = new HbaseToGCS();

        Exception exception =
                assertThrows(IllegalArgumentException.class, () -> hbaseToGCSTest.runTemplate());
        assertEquals(
                "Required parameters for HiveToGCS not passed. "
                        + "Set mandatory parameter for HiveToGCS template in "
                        + "resources/conf/template.properties file.",
                exception.getMessage());
    }

    static Stream<String> propertyKeys() {
        return Stream.of(
                HBASE_TO_GCS_FILE_FORMAT,
                HBASE_TO_GCS_SAVE_MODE,
                HBASE_TO_GCS_CATALOG);
    }


}
