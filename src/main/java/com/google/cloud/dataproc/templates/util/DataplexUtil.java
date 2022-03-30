/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.dataproc.templates.util;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.gson.GsonFactory;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spark.bigquery.repackaged.com.google.gson.JsonArray;
import com.google.cloud.spark.bigquery.repackaged.com.google.gson.JsonElement;
import com.google.cloud.spark.bigquery.repackaged.com.google.gson.JsonObject;
import com.google.cloud.spark.bigquery.repackaged.com.google.gson.JsonParser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

public class DataplexUtil {

  private static String GET_ENTITY_METHOD_URL = "https://dataplex.googleapis.com/v1/%s?view=SCHEMA";
  private static String GET_ENTITY_PARTITIONS_METHOD_URL =
      "https://dataplex.googleapis.com/v1/%s/partitions";
  private static String GET_ENTITY_LIST_METHOD_URL =
      "https://dataplex.googleapis.com/v1/%s/entities?filter=asset=%s&view=TABLES";

  private static String ASSET_ENTITIES_PROP_KEY = "entities";
  private static String ASSET_ENTITY_NAME_PROP_KEY = "name";
  private static String ENTITY_BASE_PATH_PROP_KEY = "dataPath";
  private static String ENTITY_FORMAT_PROP_KEY = "format";
  private static String ENTITY_SCHEMA_PROP_KEY = "schema";
  private static String ENTITY_SCHEMA_PARTITION_FIELDS_PROP_KEY = "partitionFields";
  private static String PARTITION_FIELD_NAME_PROP_KEY = "name";
  private static String ENTITY_PARTITION_PROP_KEY = "partitions";
  private static String ENTITY_PARTITION_LOCATION_PROP_KEY = "location";
  private static String ENTITY_PARTITION_VALUES_PROP_KEY = "values";
  private static String ENTITY_SCHEMA_FIELD_TYPE_PROP_KEY = "type";
  private static String ENTITY_SCHEMA_FIELD_MODE_PROP_KEY = "mode";
  private static String ENTITY_SCHEMA_FIELD_MODE_UNSPECIFIED = "MODE_UNSPECIFIED";
  private static String ENTITY_SCHEMA_FIELD_MODE_REPEATED = "REPEATED";
  private static String ENTITY_SCHEMA_TYPE_MODE_RECORD = "RECORD";
  private static String ENTITY_SCHEMA_FIELDS_PROP_NAME = "fields";

  private static String DATAPLEX_BOOLEAN_DATA_TYPE_NAME = "BOOLEAN";
  private static String DATAPLEX_BYTE_DATA_TYPE_NAME = "BYTE";
  private static String DATAPLEX_INT16_DATA_TYPE_NAME = "INT16";
  private static String DATAPLEX_INT32_DATA_TYPE_NAME = "INT32";
  private static String DATAPLEX_INT64_DATA_TYPE_NAME = "INT64";
  private static String DATAPLEX_FLOAT_DATA_TYPE_NAME = "FLOAT";
  private static String DATAPLEX_DOUBLE_DATA_TYPE_NAME = "DOUBLE";
  private static String DATAPLEX_DECIMAL_DATA_TYPE_NAME = "DECIMAL";
  private static String DATAPLEX_STRING_DATA_TYPE_NAME = "STRING";
  private static String DATAPLEX_BINARY_DATA_TYPE_NAME = "BINARY";
  private static String DATAPLEX_TIMESTAMP_DATA_TYPE_NAME = "TIMESTAMP";
  private static String DATAPLEX_DATE_DATA_TYPE_NAME = "DATE";

  /**
   * Execute request on Google API
   *
   * @param url of the API method
   * @return request response
   * @throws IOException when request fails
   */
  private static JsonObject executeRequest(String url) throws IOException {
    GoogleCredentials googleCredentials = GoogleCredentials.getApplicationDefault();
    HttpCredentialsAdapter credentialsAdapter = new HttpCredentialsAdapter(googleCredentials);
    HttpRequestFactory requestFactory =
        new NetHttpTransport().createRequestFactory(credentialsAdapter);
    HttpRequest request = requestFactory.buildGetRequest(new GenericUrl(url));
    JsonObjectParser parser = new JsonObjectParser(GsonFactory.getDefaultInstance());
    request.setParser(parser);
    HttpResponse response = request.execute();
    String resp = response.parseAsString();
    return JsonParser.parseString(resp).getAsJsonObject();
  }

  /**
   * Execute request on Google API
   *
   * @param asset
   * @return list of entities in the asset
   * @throws IOException when request fails
   */
  public static List<String> getEntityNameListFromAsset(String asset) throws IOException {
    List<String> entityList = new ArrayList<String>();
    String url =
        String.format(
            GET_ENTITY_LIST_METHOD_URL, asset.split("/assets/")[0], asset.split("/assets/")[1]);
    String urlWithTokenParam = url;
    while (true) {
      JsonObject resp = executeRequest(urlWithTokenParam);
      Iterator entityIterator = resp.get(ASSET_ENTITIES_PROP_KEY).getAsJsonArray().iterator();
      while (entityIterator.hasNext()) {
        JsonObject currentEntity = (JsonObject) entityIterator.next();
        entityList.add(currentEntity.get(ASSET_ENTITY_NAME_PROP_KEY).getAsString());
      }

      if (!resp.has("nextPageToken")) {
        break;
      } else {
        urlWithTokenParam = url + "&pageToken=" + resp.get("nextPageToken").getAsString();
      }
    }
    return entityList;
  }
  /**
   * Execute request on Google API to fetch schema of a Dataplex entity
   *
   * @param entity name
   * @return entity schema
   * @throws IOException when request on Dataplex API fails
   */
  public static JsonObject getEntitySchema(String entity) throws IOException {
    String url = String.format(GET_ENTITY_METHOD_URL, entity);
    return executeRequest(url);
  }

  /**
   * Execute request on Google API to fetch partitions of a Dataplex entity
   *
   * @param entity name
   * @return entity partitions
   * @throws IOException when request on Dataplex API fails
   */
  public static JsonObject getEntityPartitions(String entity) throws IOException {
    String url = String.format(GET_ENTITY_PARTITIONS_METHOD_URL, entity);
    return executeRequest(url);
  }

  /**
   * Execute request on Google API to fetch schema of a Dataplex entity and parses out base path of
   * entity data
   *
   * @param entity name
   * @return source data base path
   * @throws IOException when request on Dataplex API fails
   */
  public static String getBasePathEntityData(String entity) throws IOException {
    JsonObject responseJson = DataplexUtil.getEntitySchema(entity);
    return responseJson.get(ENTITY_BASE_PATH_PROP_KEY).getAsString();
  }

  /**
   * Execute request on Google API to fetch schema of a Dataplex entity and parses out file format
   *
   * @param entity name
   * @return file format
   * @throws IOException when request on Dataplex API fails
   */
  public static String getInputFileFormat(String entity) throws IOException {
    JsonObject responseJson = DataplexUtil.getEntitySchema(entity);
    return responseJson
        .getAsJsonObject(ENTITY_FORMAT_PROP_KEY)
        .get(ENTITY_FORMAT_PROP_KEY)
        .getAsString()
        .toLowerCase();
  }

  /**
   * Execute request on Google API to fetch schema of a Dataplex entity and parses out a list of
   * partition Keys
   *
   * @param entity name
   * @return list with partition keys of the entity
   * @throws IOException when request on Dataplex API fails
   */
  public static List<String> getPartitionKeyList(String entity) throws IOException {
    JsonObject responseJson = DataplexUtil.getEntitySchema(entity);
    JsonArray partitionKeys =
        responseJson
            .getAsJsonObject(ENTITY_SCHEMA_PROP_KEY)
            .getAsJsonArray(ENTITY_SCHEMA_PARTITION_FIELDS_PROP_KEY);

    List<String> partitionFieldsNames = new ArrayList<String>();
    Iterator partitionFieldsIter = partitionKeys.iterator();
    while (partitionFieldsIter.hasNext()) {
      JsonObject partitionField = (JsonObject) partitionFieldsIter.next();
      partitionFieldsNames.add(partitionField.get(PARTITION_FIELD_NAME_PROP_KEY).getAsString());
    }
    return partitionFieldsNames;
  }

  /**
   * Execute request on Google API to fetch partitions of a Dataplex entity and parses out a list
   * with all partitions each element contains gcs path and key values for a given partition This
   * will return a list of string with the pattern: ["partition_path,key_1,key_2,...,key_n",
   * "partition_path,key1,key2,...,keyn", ...]
   *
   * @param entity name
   * @return list of all partitions where each element contains gcs path and key values for a given
   *     partition
   * @throws IOException when request on Dataplex API fails
   */
  public static List<String> getPartitionsListWithLocationAndKeys(String entity)
      throws IOException {
    JsonObject responseJson = getEntityPartitions(entity);

    Iterator<JsonElement> partitionsIterator =
        responseJson.getAsJsonArray(ENTITY_PARTITION_PROP_KEY).iterator();
    List<String> partitionsListWithLocationAndKeys = new ArrayList<String>();
    while (partitionsIterator.hasNext()) {
      JsonObject partition = (JsonObject) partitionsIterator.next();

      String currentRecord = "";
      currentRecord += partition.get(ENTITY_PARTITION_LOCATION_PROP_KEY).getAsString();

      Iterator<JsonElement> partitionValuesIterator =
          partition.get(ENTITY_PARTITION_VALUES_PROP_KEY).getAsJsonArray().iterator();
      while (partitionValuesIterator.hasNext()) {
        currentRecord += "," + partitionValuesIterator.next().getAsString();
      }

      partitionsListWithLocationAndKeys.add(currentRecord);
    }
    return partitionsListWithLocationAndKeys;
  }

  /**
   * Builds a hasmap with mapping between Dataplex datatype name and spark DataType
   *
   * @return hasmap with mapping between Dataplex datatype name and spark DataType
   */
  public static HashMap<String, DataType> getDataplexTypeToSparkTypeMap() {
    HashMap<String, DataType> dataplexTypeToSparkType = new HashMap<String, DataType>();
    dataplexTypeToSparkType.put(DATAPLEX_BOOLEAN_DATA_TYPE_NAME, DataTypes.BooleanType);
    dataplexTypeToSparkType.put(DATAPLEX_BYTE_DATA_TYPE_NAME, DataTypes.ByteType);
    dataplexTypeToSparkType.put(DATAPLEX_INT16_DATA_TYPE_NAME, DataTypes.IntegerType);
    dataplexTypeToSparkType.put(DATAPLEX_INT32_DATA_TYPE_NAME, DataTypes.IntegerType);
    dataplexTypeToSparkType.put(DATAPLEX_INT64_DATA_TYPE_NAME, DataTypes.IntegerType);
    dataplexTypeToSparkType.put(DATAPLEX_FLOAT_DATA_TYPE_NAME, DataTypes.FloatType);
    dataplexTypeToSparkType.put(DATAPLEX_DOUBLE_DATA_TYPE_NAME, DataTypes.DoubleType);
    dataplexTypeToSparkType.put(DATAPLEX_DECIMAL_DATA_TYPE_NAME, DataTypes.createDecimalType());
    dataplexTypeToSparkType.put(DATAPLEX_STRING_DATA_TYPE_NAME, DataTypes.StringType);
    dataplexTypeToSparkType.put(DATAPLEX_BINARY_DATA_TYPE_NAME, DataTypes.BinaryType);
    dataplexTypeToSparkType.put(DATAPLEX_TIMESTAMP_DATA_TYPE_NAME, DataTypes.TimestampType);
    dataplexTypeToSparkType.put(DATAPLEX_DATE_DATA_TYPE_NAME, DataTypes.DateType);
    return dataplexTypeToSparkType;
  }

  /**
   * Builds a spark schema that matches the schema of the source Dataplex entity
   *
   * @param schema to which fields will added
   * @param dataplexTypeToSparkType mapping between Dataplex datatype name and spark DataType
   * @param dataplexSchema schema from source Dataplex entity
   * @return a spark schema matching Dataples source entity schema
   */
  public static List<StructField> buildSparkSchemaFromDataplexSchema(
      List<StructField> schema,
      HashMap<String, DataType> dataplexTypeToSparkType,
      JsonArray dataplexSchema) {

    Iterator fieldsIterator = dataplexSchema.iterator();
    while (fieldsIterator.hasNext()) {
      JsonObject field = (JsonObject) fieldsIterator.next();
      String type = field.get(ENTITY_SCHEMA_FIELD_TYPE_PROP_KEY).getAsString();
      String name = field.get(PARTITION_FIELD_NAME_PROP_KEY).getAsString();
      String mode = ENTITY_SCHEMA_FIELD_MODE_UNSPECIFIED;
      if (field.get(ENTITY_SCHEMA_FIELD_MODE_PROP_KEY) != null) {
        mode = field.get(ENTITY_SCHEMA_FIELD_MODE_PROP_KEY).getAsString();
      }

      if (type.equals(ENTITY_SCHEMA_TYPE_MODE_RECORD)
          && mode.equals(ENTITY_SCHEMA_FIELD_MODE_REPEATED)) {
        List<StructField> structFieldList = new ArrayList<>();
        JsonArray nestedField = field.getAsJsonArray(ENTITY_SCHEMA_FIELDS_PROP_NAME);
        structFieldList =
            buildSparkSchemaFromDataplexSchema(
                structFieldList, dataplexTypeToSparkType, nestedField);
        StructField newField =
            DataTypes.createStructField(
                name, DataTypes.createArrayType(DataTypes.createStructType(structFieldList)), true);
        schema.add(newField);
      } else if (type.equals(ENTITY_SCHEMA_TYPE_MODE_RECORD)) {
        List<StructField> structFieldList = new ArrayList<>();
        JsonArray nestedField = field.getAsJsonArray(ENTITY_SCHEMA_FIELDS_PROP_NAME);
        structFieldList =
            buildSparkSchemaFromDataplexSchema(
                structFieldList, dataplexTypeToSparkType, nestedField);
        StructField newField =
            DataTypes.createStructField(name, DataTypes.createStructType(structFieldList), true);
        schema.add(newField);
      } else if (mode.equals(ENTITY_SCHEMA_FIELD_MODE_REPEATED)) {
        StructField newField =
            DataTypes.createStructField(
                name, DataTypes.createArrayType(dataplexTypeToSparkType.get(type)), true);
        schema.add(newField);
      } else {
        StructField newField =
            DataTypes.createStructField(name, dataplexTypeToSparkType.get(type), true);
        schema.add(newField);
      }
    }
    return schema;
  }

  /**
   * Cast fields in a dataset to match datatypes of source Dataplex entity schema
   *
   * @param inputDS dataset that will be casted to new schema
   * @param entity dataplex source entity
   * @return a spark dataset with schema matching Dataples source entity schema
   */
  public static Dataset<Row> castDatasetToDataplexSchema(Dataset<Row> inputDS, String entity)
      throws IOException {
    JsonObject entityJson = getEntitySchema(entity);
    HashMap<String, DataType> dataplexTypeToSparkType = getDataplexTypeToSparkTypeMap();
    JsonArray dataplexSchema =
        entityJson
            .getAsJsonObject(ENTITY_SCHEMA_PROP_KEY)
            .getAsJsonArray(ENTITY_SCHEMA_FIELDS_PROP_NAME);
    JsonArray dataplexPartitionSchema =
        entityJson
            .getAsJsonObject(ENTITY_SCHEMA_PROP_KEY)
            .getAsJsonArray(ENTITY_SCHEMA_PARTITION_FIELDS_PROP_KEY);

    if (dataplexPartitionSchema != null) {
      dataplexSchema.addAll(dataplexPartitionSchema);
    }

    List<StructField> schema = new ArrayList<>();
    schema = buildSparkSchemaFromDataplexSchema(schema, dataplexTypeToSparkType, dataplexSchema);
    List<String> selectExpresions =
        schema.stream()
            .map(
                field ->
                    String.format(
                        "CAST ( %s AS %s) %s", field.name(), field.dataType().sql(), field.name()))
            .collect(Collectors.toList());

    return inputDS.selectExpr(selectExpresions.toArray(new String[0]));
  }
}
