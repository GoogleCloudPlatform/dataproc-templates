package com.customer.util;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.Map;
import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import scala.collection.Seq;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.HashSet;
import java.util.Collections;
import java.util.Set;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import static org.apache.spark.sql.functions.lit;

public class ClassPathLister {
    private static final String PROJECT_ID = "dataproc-templates";
    private static final String BIGQUERY_OUTPUT_TABLE = "bigquery.output.table";
    private static final String BIGQUERY_TEMP_GCS_BUCKET = "bigquery.temp.gcs.bucket";
    public static void main(String[] args) throws IOException {

        String classPath = System.getProperty("java.class.path");
        String[] classPathEntries = classPath.split(File.pathSeparator);
        System.out.println("Number of elements in Classpath: " + classPathEntries.length);
        SparkSession spark = SparkSession.builder().appName("ClassPathLister").getOrCreate();
        
        /* Spark Context does not have any dataproc cluster or serverless specific properties
        Tuple2<String, String> [] scalaProps=spark.sparkContext().getConf().getAll();
        Map<String, String> sparkProps = Arrays.stream(scalaProps)
            .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
        for (Map.Entry<String, String> sparkProp : sparkProps.entrySet()) {
            System.out.println(sparkProp.getKey() + "=" + sparkProp.getValue());
        }  */

        StructType schema = new StructType()
            .add("fileName", DataTypes.StringType, true)
            .add("implementationTitle", DataTypes.StringType, true)
            .add("implementationVersion", DataTypes.StringType, true);
        Dataset<Row> df = spark.createDataFrame(Collections.emptyList(), schema);

        for (String entry : classPathEntries) {
            File file = new File(entry);
            if (file.isDirectory()) {
                // Using a lambda expression to filter for .jar files
                FilenameFilter filter = (dir, name) -> name.toLowerCase().endsWith(".jar");
                File [] files = file.listFiles(filter);
                for (File jarFile : files) {
                    df = df.union(listJar(spark, jarFile, schema));
                }
            }
            else if (file.isFile() && entry.toLowerCase().endsWith(".jar")) {
                 df = df.union(listJar(spark, file, schema));
            }
        }

        Map<String, String> envVars = System.getenv();
        HashMap<String, String> handyVars = new HashMap<String, String>(envVars);
        Set<String> valuableKeys = new HashSet<>();
        Collections.addAll(valuableKeys, "DATAPROC_WORKLOAD_TYPE", "DATAPROC_IMAGE_VERSION", "DATAPROC_IMAGE_TYPE", "SPARK_SCALA_VERSION");
        handyVars.keySet().retainAll(valuableKeys);
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String jsonString = objectMapper.writeValueAsString(handyVars);
            df = df.withColumn("version", lit(jsonString));
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            e.printStackTrace();
        }

        Properties properties = new Properties();
        properties.load(new StringReader(String.join("\n", args)));
        df.write()
            .format("bigquery")
            .option("temporaryGcsBucket",properties.getProperty(BIGQUERY_TEMP_GCS_BUCKET))
            .option("table", properties.getProperty(BIGQUERY_OUTPUT_TABLE))
            .mode(SaveMode.valueOf("Append"))
            .save();
        spark.stop();
    }
    
    public static Dataset<Row> listJar(SparkSession spark, File file, StructType schema) {
        try (JarFile jarFile = new JarFile(file)) {
            Manifest manifest = jarFile.getManifest();
            String implementationTitle;
            String implementationVersion;
            if (manifest != null) {
                Attributes attributes = manifest.getMainAttributes();
                implementationTitle = attributes.getValue("Implementation-Title");
                implementationVersion = attributes.getValue("Implementation-Version");

                if (implementationTitle == null || implementationTitle.trim().isEmpty()) {
                    implementationTitle = null;
                }

                if (implementationVersion == null || implementationVersion.trim().isEmpty()) {
                    implementationVersion = null;
                }

            } else {
                implementationTitle = null;
                implementationVersion = null;
            }

            Row row = RowFactory.create(file.getName(), implementationTitle, implementationVersion);
            return spark.createDataFrame(Collections.singletonList(row), schema);

        } catch (IOException e) {
            System.err.println("Error reading JAR file: " + file.getName());
            e.printStackTrace();
            Row row = RowFactory.create(file.getName(), "Error", e.getMessage());
            return spark.createDataFrame(Collections.singletonList(row), schema);
        }
    }
    
    /**
     * Retrieves a secret from Google Secret Manager.
     *
     * @param secretId The ID of the secret to retrieve.
     * @return The secret value.
     * @throws IOException If an I/O error occurs.
     */
    private static String getSecret(String secretId) throws IOException {
        try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
            SecretVersionName secretVersionName = SecretVersionName.of(PROJECT_ID, secretId, "latest");
            AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
            return response.getPayload().getData().toStringUtf8();
        }
    }
}
