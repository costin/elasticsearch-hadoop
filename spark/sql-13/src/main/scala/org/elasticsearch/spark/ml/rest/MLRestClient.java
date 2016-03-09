/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.spark.ml.rest;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.Request;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.util.BytesArray;

public class MLRestClient extends RestClient {

    public MLRestClient(Settings setting) {
        super(setting);
    }

    public List<String> analyze(String path, String body) throws IOException {
        InputStream stream = execute(Request.Method.POST, path, new BytesArray(body)).body();
        return parseAnalyzeAPI(stream);
    }

    protected List<String> parseAnalyzeAPI(InputStream stream) {
        // ugly parsing
        List<Map<String, Object>> tokens = parseContent(stream, "tokens");
        List<String> justTheTokens = new ArrayList<String>(tokens.size());
        for (Map<String, Object> token : tokens) {
            justTheTokens.add(token.get("token").toString());
        }
        return justTheTokens;
    }
}
