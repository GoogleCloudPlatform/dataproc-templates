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
package com.google.cloud.dataproc.templates;

import com.google.cloud.dataproc.templates.util.PropertyUtil;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.streaming.StreamingQueryException;

/** Base class for Dataproc templates. */
public interface BaseTemplate {

  /** List of all templates. */
  enum TemplateName {
    WORDCOUNT,
    HIVETOGCS,
    PUBSUBTOBQ,
    SPANNERTOGCS,
    GCSTOSPANNER,
    HIVETOBIGQUERY,
    S3TOBIGQUERY,
    GCSTOBIGQUERY,
    GCSTOGCS,
    JDBCTOBIGQUERY,
    JDBCTOGCS,
    BIGQUERYTOGCS,
    GENERAL,
    DATAPLEXGCSTOBQ,
    PUBSUBTOGCS,
    HBASETOGCS,
    GCSTOJDBC,
    KAFKATOBQ,
    KAFKATOGCS,
    CASSANDRATOBQ,
    CASSANDRATOGCS,
    REDSHIFTTOGCS,
    SNOWFLAKETOGCS,
    JDBCTOSPANNER,
    PUBSUBTOBIGTABLE,
    GCSTOBIGTABLE
  }

  default Properties getProperties() {
    return PropertyUtil.getProperties();
  }

  void validateInput() throws Exception;
  /** Executes the template. */
  void runTemplate() throws StreamingQueryException, TimeoutException;
}
