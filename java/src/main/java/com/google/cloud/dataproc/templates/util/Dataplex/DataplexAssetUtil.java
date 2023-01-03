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
package com.google.cloud.dataproc.templates.util.Dataplex;

import com.google.cloud.spark.bigquery.repackaged.com.google.gson.JsonObject;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataplexAssetUtil {

  public String assetName;
  private JsonObject assetDetails;

  private final String GET_ASSET_METHOD_URL = "https://dataplex.googleapis.com/v1/%s";
  private final String RESOURCE_SPEC_PROP_KEY = "resourceSpec";
  private final String NAME_PROP_KEY = "name";
  private final String DATASET_FULLNAME_REGEX =
      "projects\\/([a-z0-9\\-]+)\\/datasets\\/([a-zA-Z0-9_]+)";
  private String datasetName;
  private String projectId;

  /**
   * Constructs a new DataplexAssetUtil based on a Dataplex assetName
   *
   * @param assetName - the name of the Dataplex asset. Should have the format:
   *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity_id_1}
   */
  public DataplexAssetUtil(String assetName) throws IOException {
    this.assetName = assetName;
    this.assetDetails = getAssetDetails();
    setDatasetFullname();
  }

  /**
   * Execute request on Google API to fetch details of a Dataplex asset
   *
   * @return asset details
   * @throws IOException when request on Dataplex API fails
   */
  private JsonObject getAssetDetails() throws IOException {
    String url = String.format(GET_ASSET_METHOD_URL, this.assetName);
    return DataplexAPIUtil.executeRequest(url);
  }

  /**
   * Parse Dataplex asset details to extract project id and dataset name, these values will be used
   * to ser the fields projectId and datasetName
   *
   * @return entity schema
   * @throws IOException when request on Dataplex API fails
   */
  private void setDatasetFullname() {
    String dataPath =
        this.assetDetails.getAsJsonObject(RESOURCE_SPEC_PROP_KEY).get(NAME_PROP_KEY).getAsString();
    Pattern p = Pattern.compile(DATASET_FULLNAME_REGEX);
    Matcher m = p.matcher(dataPath);
    m.find();
    this.projectId = m.group(1);
    this.datasetName = m.group(2);
  }

  public String getDatasetName() {
    return this.datasetName;
  }

  public String getProjectId() {
    return this.projectId;
  }
}
