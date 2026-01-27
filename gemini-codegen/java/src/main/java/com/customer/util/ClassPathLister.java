package com.customer.util;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;
import scala.Tuple2;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_binary;

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
            .add("implementationVersion", DataTypes.StringType, true)
            .add("GAV", DataTypes.StringType, true)
            .add("classes", new ArrayType(DataTypes.StringType, true), true);
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
        Map<String, String> handyVars = new HashMap<>(envVars);
        Set<String> valuableKeys = new HashSet<>();
        Collections.addAll(valuableKeys, "DATAPROC_WORKLOAD_TYPE", "DATAPROC_IMAGE_VERSION", "DATAPROC_IMAGE_TYPE", "SPARK_SCALA_VERSION");
        handyVars.keySet().retainAll(valuableKeys);
        Pattern p = Pattern.compile("^\\D+(\\d+\\.\\d+\\.\\d+)$");
        String scalaVersionString = scala.util.Properties.versionString();
        Matcher m = p.matcher(scalaVersionString);
        if (m.find()) {
            handyVars.put("SCALA_VERSION", m.group(1));
        }
        handyVars.put("SPARK_VERSION", spark.version());
        handyVars.put("JAVA_VERSION", System.getProperty("java.version"));

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            // Add columns specifying type of job, version (if available) and environment variables
            // Ensure columns are nullable.
            String[] exCols = df.columns();
            if (handyVars.containsKey("DATAPROC_IMAGE_VERSION")) {
                df = df.select(col(exCols[0]), col(exCols[1]),col(exCols[2]), col(exCols[3]),col(exCols[4]),
                    functions.when(functions.lit(true), functions.lit("Cluster Job")).alias("workloadType"),
                    functions.when(functions.lit(true), functions.lit(handyVars.get("DATAPROC_IMAGE_VERSION"))).alias("imageVersion"),
                    functions.when(functions.lit(true), functions.lit(objectMapper.writeValueAsString(handyVars))).alias("environVars"));
            }
            else if (handyVars.containsKey("DATAPROC_WORKLOAD_TYPE")) {
                df = df.select(col(exCols[0]), col(exCols[1]),col(exCols[2]), col(exCols[3]),col(exCols[4]),
                    functions.when(functions.lit(true), functions.lit("Serverless " + handyVars.get("DATAPROC_WORKLOAD_TYPE"))).alias("workloadType"),
                    functions.when(functions.lit(true), functions.lit(null).cast(DataTypes.StringType)).alias("imageVersion"),
                    functions.when(functions.lit(true), functions.lit(objectMapper.writeValueAsString(handyVars))).alias("environVars"));
            }
            else {
                df = df.select(col(exCols[0]), col(exCols[1]),col(exCols[2]), col(exCols[3]),col(exCols[4]),
                    functions.when(functions.lit(true), functions.lit(null).cast(DataTypes.StringType)).alias("workloadType"),
                    functions.when(functions.lit(true), functions.lit(null).cast(DataTypes.StringType)).alias("imageVersion"),
                    functions.when(functions.lit(true), functions.lit(objectMapper.writeValueAsString(handyVars))).alias("environVars"));

            }
            df.show();
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
            List<String> defClasses = new ArrayList<>();
            Enumeration<JarEntry> entries = jarFile.entries();
            Map<String, String> POMmap = new HashMap<> ();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();
                if (name.startsWith("META-INF/maven/") && name.endsWith("/pom.properties")) {
                    try (InputStream inputStream = jarFile.getInputStream(entry)) {
                        Properties props = new Properties();
                        props.load(inputStream);
                        if (props.getProperty("groupId")!= null) {
                            POMmap.put("groupId", props.getProperty("groupId"));
                            POMmap.put("artifactId", props.getProperty("artifactId"));
                            POMmap.put("version", props.getProperty("version"));
                        }
                    }
                }
                else if (name.endsWith(".class")) {
                    defClasses.add(entry.getName()
                                    .replace("/", ".")
                                    .replace(".class", ""));
                }
            }
            ObjectMapper objectMapper = new ObjectMapper();
            if (POMmap.isEmpty()) {
                Row row = RowFactory.create(file.getName(), implementationTitle, implementationVersion,
                        objectMapper.writeValueAsString(getGroupIdAndArtifactId(DigestUtils.sha1Hex(new FileInputStream(file)))),
                        defClasses);
                return spark.createDataFrame(Collections.singletonList(row), schema);
            }
            else {
                Row row = RowFactory.create(file.getName(), implementationTitle, implementationVersion,
                                objectMapper.writeValueAsString(POMmap),defClasses);
                return spark.createDataFrame(Collections.singletonList(row), schema);
            }
        } catch (IOException e) {
            System.err.println("Error reading JAR file: " + file.getName());
            e.printStackTrace();
            Row row = RowFactory.create(file.getName(), "Error", e.getMessage(), null, null);
            return spark.createDataFrame(Collections.singletonList(row), schema);
        }

    }

    /**
     * Finds the groupId and artifactId for a given SHA-1 checksum.
     *
     * @param sha1Checksum The SHA-1 checksum of the JAR file.
     * @return A string containing the groupId and artifactId, or null if not found.
     * @throws IOException If there is an issue with the HTTP connection.
     */
    public static Map<String, String> getGroupIdAndArtifactId(String sha1Checksum) throws IOException {
        final String MAVEN_SEARCH_API_URL = "https://search.maven.org/solrsearch/select?q=1:%s&rows=20&wt=json";
        URL url = new URL(String.format(MAVEN_SEARCH_API_URL, sha1Checksum));
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");

        int responseCode = connection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuilder content = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();
            connection.disconnect();

            return parseMavenResponse(content.toString());
        } else {
            connection.disconnect();
            throw new IOException("Failed to fetch data from Maven Central. Response code: " + responseCode);
        }
    }

    private static Map<String, String> parseMavenResponse(String jsonResponse) {
        // A simple parser to avoid adding a JSON library dependency.
        // This looks for "g":"...", "a":"..." and extracts the values.
        String groupId = "";
        String artifactId = "";
        String version = "";

        String gPattern = "\"g\":\"";
        int gIndex = jsonResponse.indexOf(gPattern);
        if (gIndex != -1) {
            int gStartIndex = gIndex + gPattern.length();
            int gEndIndex = jsonResponse.indexOf("\"", gStartIndex);
            if (gEndIndex != -1) {
                groupId = jsonResponse.substring(gStartIndex, gEndIndex);
            }
        }

        String aPattern = "\"a\":\"";
        int aIndex = jsonResponse.indexOf(aPattern);
        if (aIndex != -1) {
            int aStartIndex = aIndex + aPattern.length();
            int aEndIndex = jsonResponse.indexOf("\"", aStartIndex);
            if (aEndIndex != -1) {
                artifactId = jsonResponse.substring(aStartIndex, aEndIndex);
            }
        }
        String vPattern = "\"v\":\"";
        int vIndex = jsonResponse.indexOf(vPattern);
        if (vIndex != -1) {
            int vStartIndex = vIndex + vPattern.length();
            int vEndIndex = jsonResponse.indexOf("\"", vStartIndex);
            if (vEndIndex != -1) {
                version = jsonResponse.substring(vStartIndex, vEndIndex);
            }
        }
        return Map.of(
                "groupId", groupId,
                "artifactId", artifactId,
                "version", version
                );
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
