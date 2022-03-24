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
import java.util.Iterator;
import java.util.List;

public class DataplexUtil {

  /**
   * Execute request on Google API
   *
   * @param url of the API method
   * @return request response
   * @throws IOException when request fails
   */
  private static HttpResponse executeRequest(String url) throws IOException {
    GoogleCredentials googleCredentials = GoogleCredentials.getApplicationDefault();
    HttpCredentialsAdapter credentialsAdapter = new HttpCredentialsAdapter(googleCredentials);
    HttpRequestFactory requestFactory =
        new NetHttpTransport().createRequestFactory(credentialsAdapter);
    HttpRequest request = requestFactory.buildGetRequest(new GenericUrl(url));
    JsonObjectParser parser = new JsonObjectParser(GsonFactory.getDefaultInstance());
    request.setParser(parser);
    return request.execute();
  }

  /**
   * Execute request on Google API to fetch schema of a Dataplex entity
   *
   * @param entity name
   * @return entity schema
   * @throws IOException when request on Dataplex API fails
   */
  public static HttpResponse getEntitySchema(String entity) throws IOException {
    String url = "https://dataplex.googleapis.com/v1/" + entity + "?view=SCHEMA";
    return executeRequest(url);
  }

  /**
   * Execute request on Google API to fetch partitions of a Dataplex entity
   *
   * @param entity name
   * @return entity partitions
   * @throws IOException when request on Dataplex API fails
   */
  public static HttpResponse getEntityPartitions(String entity) throws IOException {
    String url = "https://dataplex.googleapis.com/v1/" + entity + "/partitions";
    return executeRequest(url);
  }

  /**
   * Execute request on Google API to fetch schema of a Dataplex entity and parses out partition
   * Keys
   *
   * @param entity name
   * @return entity partition keys
   * @throws IOException when request on Dataplex API fails
   */
  public static List<String> getPartitionKeyList(String entity) throws IOException {
    HttpResponse response = DataplexUtil.getEntitySchema(entity);
    String resp = response.parseAsString();
    JsonObject responseJson = JsonParser.parseString(resp).getAsJsonObject();
    JsonArray partitionKeys =
        responseJson.getAsJsonObject("schema").getAsJsonArray("partitionFields");

    List<String> partitionFieldsNames = new ArrayList<String>();
    Iterator partitionFieldsIter = partitionKeys.iterator();
    while (partitionFieldsIter.hasNext()) {
      JsonObject partitionField = (JsonObject) partitionFieldsIter.next();
      partitionFieldsNames.add(partitionField.get("name").getAsString());
    }
    return partitionFieldsNames;
  }

  /**
   * Execute request on Google API to fetch partitions of a Dataplex entity and parses out a list
   * with all partitions each element contains gcs path and key values for a given partition
   *
   * @param entity name
   * @return list of all partitions where each element contains gcs path and key values for a given
   *     partition
   * @throws IOException when request on Dataplex API fails
   */
  public static List<String> getPartitionsListWithLocationAndKeys(String entity)
      throws IOException {
    HttpResponse response = getEntityPartitions(entity);
    String resp = response.parseAsString();
    JsonObject responseJson = JsonParser.parseString(resp).getAsJsonObject();

    Iterator<JsonElement> partitionsIterator = responseJson.getAsJsonArray("partitions").iterator();
    List<String> partitionsListWithLocationAndKeys = new ArrayList<String>();
    while (partitionsIterator.hasNext()) {
      JsonObject partition = (JsonObject) partitionsIterator.next();

      String currentRecord = "";
      currentRecord += partition.get("location").getAsString();

      Iterator<JsonElement> partitionValuesIterator =
          partition.get("values").getAsJsonArray().iterator();
      while (partitionValuesIterator.hasNext()) {
        currentRecord += "," + partitionValuesIterator.next().getAsString();
      }

      partitionsListWithLocationAndKeys.add(currentRecord);
    }
    return partitionsListWithLocationAndKeys;
  }
}
