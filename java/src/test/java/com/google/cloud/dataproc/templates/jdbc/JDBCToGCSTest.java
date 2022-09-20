package com.google.cloud.dataproc.templates.jdbc;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil;

public class JDBCToGCSTest {

	private JDBCToGCS jdbcToGCSTest;
	
	private static final Logger logger = LoggerFactory.getLogger(JDBCToGCSTest.class);
	
    @BeforeEach
    void setUp() {
      System.setProperty("hadoop.home.dir", "/");
      SparkSession spark = SparkSession.builder().master("local").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate();
    }
    
    @Test
    void runTemplateWithValidParameters(){
        logger.info("Running test: runTemplateWithValidParameters");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_OUTPUT_LOCATION, "gs://tests/file");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_OUTPUT_FORMAT, "csv");
        PropertyUtil.getProperties().setProperty(PROJECT_ID_PROP, "append"); 
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_JDBC_URL, "append");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_JDBC_DRIVER_CLASS_NAME, "bqtable");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_SQL, "append");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_WRITE_MODE, "bqtable");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_SQL_PARTITION_COLUMN, "append");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_SQL_LOWER_BOUND, "bqtable");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_SQL_UPPER_BOUND, "append");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_SQL, "append");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_WRITE_MODE, "Overwrite");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_SQL_PARTITION_COLUMN, "append");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_SQL_LOWER_BOUND, "bqtable");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_SQL_UPPER_BOUND, "append");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_SQL_NUM_PARTITIONS, "bqtable");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_OUTPUT_PARTITION_COLUMN, "append");
        
        JDBCToGCSConfig config = JDBCToGCSConfig.fromProperties(PropertyUtil.getProperties());
        ValidationUtil.validateOrThrow(config);
    }
    
    @Test
    void runTemplateWithoutValidParameters(){
        logger.info("Running test: runTemplateWithValidParameters");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_OUTPUT_LOCATION, "gs://tests");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_OUTPUT_FORMAT, "csv");
        PropertyUtil.getProperties().setProperty(PROJECT_ID_PROP, "append"); 
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_JDBC_URL, "append");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_JDBC_DRIVER_CLASS_NAME, "bqtable");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_SQL, "append");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_WRITE_MODE, "bqtable");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_SQL_PARTITION_COLUMN, "append");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_SQL_LOWER_BOUND, "bqtable");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_SQL_UPPER_BOUND, "append");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_SQL, "append");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_WRITE_MODE, "Overwrite");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_SQL_PARTITION_COLUMN, "append");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_SQL_LOWER_BOUND, "bqtable");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_SQL_UPPER_BOUND, "append");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_SQL_NUM_PARTITIONS, "bqtable");
        PropertyUtil.getProperties().setProperty(JDBC_TO_GCS_OUTPUT_PARTITION_COLUMN, "append");
        
        JDBCToGCSConfig config = JDBCToGCSConfig.fromProperties(PropertyUtil.getProperties());
        Exception exception =
                assertThrows(IllegalArgumentException.class, () -> ValidationUtil.validateOrThrow(config));
            assertEquals(
                "Invalid object { gcsOutputLocation='gs://tests', gcsOutputFormat='csv', projectId='append', j"
                + "dbcDriverClassName='bqtable', jdbcSQL='append', jdbcSQLFile='', gcsWriteMode='Overwrite', gcsPartitionColumn='append', jdbcSQLLowerBound='bqtable', jdbcSQLUpperBound='append', jdbcSQLNumPartitions='bqtable', jdbcSQLPartitionColumn='append'}, violations [ConstraintViolationImpl{interpolatedMessage='must match \"gs://(.*?)/(.*)\"', propertyPath=gcsOutputLocation, rootBeanClass=class com.google.cloud.dataproc.templates.jdbc.JDBCToGCSConfig, messageTemplate='{jakarta.validation.constraints.Pattern.message}'}]"
                ,
                exception.getMessage());
    }
}

    