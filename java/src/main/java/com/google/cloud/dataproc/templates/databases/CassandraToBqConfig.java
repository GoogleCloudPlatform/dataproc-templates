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
package com.google.cloud.dataproc.templates.databases;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;
import java.util.regex.Matcher;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

public class CassandraToBqConfig {

    static final ObjectMapper mapper =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @JsonProperty(value = CASSANDRA_TO_BQ_INPUT_KEYSPACE)
    @NotEmpty
    private String keyspace;

    @JsonProperty(value = CASSANDRA_TO_BQ_INPUT_TABLE)
    @NotEmpty
    private String inputTable;

    @JsonProperty(value = CASSANDRA_TO_BQ_INPUT_HOST)
    @NotEmpty
    private String host;

    @JsonProperty(value = CASSANDRA_TO_BQ_BIGQUERY_LOCATION)
    @NotEmpty
    private String bqLocation;

    @JsonProperty(value = CASSANDRA_TO_BQ_WRITE_MODE)
    @NotEmpty
    @Pattern(regexp = "(?i)(Overwrite|ErrorIfExists|Append|Ignore)")
    private String mode;

    @JsonProperty(value = CASSANDRA_TO_BQ_TEMP_LOCATION)
    @NotEmpty
    private String templocation;

    @JsonProperty(value = CASSANDRA_TO_BQ_QUERY)
    private String query;

    @JsonProperty(value = CASSANDRA_TO_BQ_CATALOG)
    private String catalog="casscon";


    public String getGcsOutputLocation() {
        return gcsOutputLocation;
    }

    public String getGcsOutputFormat() {
        return gcsOutputFormat;
    }

    public String getProjectId() {
        return projectId;
    }

    public String getJdbcURL() {
        return jdbcURL;
    }

    public String getJdbcDriverClassName() {
        return jdbcDriverClassName;
    }

    public String getJdbcSQL() {
        return jdbcSQL;
    }

    public String getJdbcSQLFile() {
        return jdbcSQLFile;
    }

    public String getGcsWriteMode() {
        return gcsWriteMode;
    }

    public String getGcsPartitionColumn() {
        return gcsPartitionColumn;
    }

    public String getJdbcSQLLowerBound() {
        return jdbcSQLLowerBound;
    }

    public String getJdbcSQLUpperBound() {
        return jdbcSQLUpperBound;
    }

    public String getJdbcSQLNumPartitions() {
        return jdbcSQLNumPartitions;
    }

    public String getJdbcSQLPartitionColumn() {
        return jdbcSQLPartitionColumn;
    }

    public String getConcatedPartitionProps() {
        return jdbcSQLPartitionColumn + jdbcSQLLowerBound + jdbcSQLUpperBound + jdbcSQLNumPartitions;
    }

    public String getSQL() {
        if (StringUtils.isNotBlank(jdbcSQL)) {
            return "(" + jdbcSQL + ") as a";
        } else {
            Matcher matches = java.util.regex.Pattern.compile("gs://(.*?)/(.*)").matcher(jdbcSQLFile);
            matches.matches();
            String bucket = matches.group(1);
            String objectPath = matches.group(2);

            Storage storage = StorageOptions.getDefaultInstance().getService();
            Blob blob = storage.get(bucket, objectPath);
            String fileContent = new String(blob.getContent());
            return "(" + fileContent + ") as a";
        }
    }

    @Override
    public String toString() {
        return "{"
                + " gcsOutputLocation='"
                + getGcsOutputLocation()
                + "'"
                + ", gcsOutputFormat='"
                + getGcsOutputFormat()
                + "'"
                + ", projectId='"
                + getProjectId()
                + "'"
                + ", jdbcDriverClassName='"
                + getJdbcDriverClassName()
                + "'"
                + ", jdbcSQL='"
                + getJdbcSQL()
                + "'"
                + ", jdbcSQLFile='"
                + getJdbcSQLFile()
                + "'"
                + ", gcsWriteMode='"
                + getGcsWriteMode()
                + "'"
                + ", gcsPartitionColumn='"
                + getGcsPartitionColumn()
                + "'"
                + ", jdbcSQLLowerBound='"
                + getJdbcSQLLowerBound()
                + "'"
                + ", jdbcSQLUpperBound='"
                + getJdbcSQLUpperBound()
                + "'"
                + ", jdbcSQLNumPartitions='"
                + getJdbcSQLNumPartitions()
                + "'"
                + ", jdbcSQLPartitionColumn='"
                + getJdbcSQLPartitionColumn()
                + "'"
                + "}";
    }

    public static CassandraToBqConfig fromProperties(Properties properties) {
        return mapper.convertValue(properties, JDBCToGCSConfig.class);
    }
}
