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
package com.google.cloud.dataproc.templates.main;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GCSToBigTableHFileTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(GCSToBigTableHFileTest.class);

  @BeforeEach
  void setUp() {}

  @AfterEach
  void tearDown() {}

  @Test
  @Disabled
  void testGetCatalog() {
    String colFamily = "cf";
    String table = "testTable";
    String keyCol = "id";
    List<String> cols = ImmutableList.of("a", "b", "c", "d");

    try {
      String catalog = null;
      // catalog = GCSToBigTableHFile.getCatalog(table,cols, keyCol, colFamily);

      // Check for substrings as order members within json is not deterministic.
      assertTrue(
          catalog.contains(
              "\"table\": {\n"
                  + "    \"namespace\": \"default\",\n"
                  + "    \"name\": \"testTable\"\n"
                  + "  }"));
      assertTrue(catalog.contains("\"col\": \"id\","));
      assertTrue(
          catalog.contains(
              "\"rowkey\": {\n"
                  + "      \"cf\": \"rowkey\",\n"
                  + "      \"col\": \"id\",\n"
                  + "      \"type\": \"string\"\n"
                  + "    }"));
    } catch (Exception e) {
      LOGGER.error("Error while getting catalog.", e);
      fail("Catalog get operation failed.");
    }
  }
}
