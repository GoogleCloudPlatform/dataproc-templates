/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.dataproc.templates.options;

import jakarta.validation.constraints.NotEmpty;

public class BigQueryOutputOptions implements BaseOptions {

  @NotEmpty private String table;
  @NotEmpty private String temporaryGcsBucket;

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getTemporaryGcsBucket() {
    return temporaryGcsBucket;
  }

  public void setTemporaryGcsBucket(String temporaryGcsBucket) {
    this.temporaryGcsBucket = temporaryGcsBucket;
  }

  @Override
  public String getOptionsPrefix() {
    return "bigquery.";
  }
}
