package com.google.cloud.dataproc.templates.jdbc;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.lf5.viewer.LogFactor5Dialog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Properties;

import com.google.cloud.dataproc.templates.util.PropertyUtil;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;


public class JDBCToBigQueryTest {
    private JDBCToBigQuery jdbcToBigQueryTest;
    private static final Logger logger = LoggerFactory.getLogger(JDBCToBigQueryTest.class);
    
    @BeforeEach
    void setUp() {
      System.setProperty("hadoop.home.dir", "/");
      SparkSession spark = SparkSession.builder().master("local").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate();
    }


    @Test
    void runTemplateWithValidParameters(){
        logger.info("Running test: runTemplateWithValidParameters");
        PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_BIGQUERY_LOCATION, "bqtable");
        PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_TEMP_GCS_BUCKET, "bqtable");
        PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_JDBC_URL, "append"); 
        PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_WRITE_MODE, "append");
        PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_JDBC_DRIVER_CLASS_NAME, "bqtable");
        PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_SQL, "append");
        PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_SQL_PARTITION_COLUMN, "bqtable");
        PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_SQL_LOWER_BOUND, "append");
        PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_SQL_UPPER_BOUND, "bqtable");
        PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_SQL_NUM_PARTITIONS, "append");
        jdbcToBigQueryTest = new JDBCToBigQuery();
        assertDoesNotThrow(jdbcToBigQueryTest::validateInput);
    }
    
    
    @Test
    void runTemplateWithInValidParameters(){
        logger.info("Running test: runTemplateWithValidParameters");
        PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_TEMP_GCS_BUCKET, "bqtable");
        PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_JDBC_URL, "append"); 
        PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_WRITE_MODE, "append");
        PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_JDBC_DRIVER_CLASS_NAME, "bqtable");
        PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_SQL, "append");
        PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_SQL_PARTITION_COLUMN, "bqtable");
        PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_SQL_LOWER_BOUND, "append");
        jdbcToBigQueryTest = new JDBCToBigQuery();
        Exception exception =
                assertThrows(IllegalArgumentException.class, () -> jdbcToBigQueryTest.validateInput());
            assertEquals(
                "Required parameters for JDBCToBQ not passed. "
                    + "Set mandatory parameter for JDBCToBQ template in "
                    + "resources/conf/template.properties file or at runtime."
                    + " Refer to jdbc/README.md for more instructions.",
                exception.getMessage());
    }
}
