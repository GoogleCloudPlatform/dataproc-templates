package com.google.cloud.dataproc.templates.gcs;

import com.google.cloud.dataproc.templates.util.PropertyUtil;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.Stream;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;
import static org.junit.jupiter.api.Assertions.*;

public class GCStoMongoTest {
    private GCStoMongo gcstoMongoTestObject;
    private static final Logger LOGGER = LoggerFactory.getLogger(GCStoMongoTest.class);
    @BeforeEach
    void setUp(){
        System.setProperty("hadoop.home.dir", "/");
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
    }
    @Test
    void runTemplateWithValidParameters(){
        LOGGER.info("Running test: runTemplateWithValidParameters");
        Properties props = PropertyUtil.getProperties();
        PropertyUtil.getProperties().setProperty(GCS_MONGO_INPUT_LOCATION,"gs://test-bucket");
        PropertyUtil.getProperties().setProperty(GCS_MONGO_INPUT_FORMAT,"csv");
        PropertyUtil.getProperties().setProperty(GCS_MONGO_URL,"");
        PropertyUtil.getProperties().setProperty(GCS_MONGO_DATABASE,"");
        PropertyUtil.getProperties().setProperty(GCS_MONGO_COLLECTION,"");
        gcstoMongoTestObject = new GCStoMongo();
        assertDoesNotThrow(gcstoMongoTestObject::validateInput);

    }

    @ParameterizedTest
    @MethodSource("propertyKeys")
    void runTemplateWithInvalidParameters(String propKey){
        LOGGER.info("Running test: runTemplateWithInvalidParameters");
        PropertyUtil.getProperties().setProperty(propKey, "");
        gcstoMongoTestObject = new GCStoMongo();
        Exception exception =
                assertThrows(IllegalArgumentException.class, () -> gcstoMongoTestObject.runTemplate());
        assertEquals(
                "Required parameters for GCStoBQ not passed. "
                        + "Set mandatory parameter for GCStoBQ template"
                        + " in resources/conf/template.properties file.",
                exception.getMessage());
    }
    static Stream<String> propertyKeys() {
        return Stream.of(
                GCS_MONGO_INPUT_LOCATION,
                GCS_MONGO_INPUT_FORMAT,
                GCS_MONGO_URL,
                GCS_MONGO_DATABASE,
                GCS_MONGO_COLLECTION);
    }
}
