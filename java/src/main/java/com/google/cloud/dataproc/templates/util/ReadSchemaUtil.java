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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadSchemaUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReadSchemaUtil.class);

  public static StructType readSchema(String schemaUrl) {

    StructType schema = null;

    String[] split_url = schemaUrl.replace("gs://", "").split("/", 2);
    String bucket = split_url[0];
    String objectUrl = split_url[1];

    Storage storage = StorageOptions.getDefaultInstance().getService();

    Blob blob = storage.get(bucket, objectUrl);
    String schemaSource = new String(blob.getContent());
    schema = (StructType) DataType.fromJson(schemaSource);

    return schema;
  }
}
