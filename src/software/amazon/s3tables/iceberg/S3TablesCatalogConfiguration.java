/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package software.amazon.s3tables.iceberg;

import org.apache.commons.configuration2.CombinedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.SystemConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.MergeCombiner;

import java.io.File;
import java.util.Iterator;

public class S3TablesCatalogConfiguration {
    private final CombinedConfiguration config;

    public S3TablesCatalogConfiguration() {
        // Initialize CombinedConfiguration with a MergeCombiner
        this.config = new CombinedConfiguration(new MergeCombiner());
    }

    // Load configuration from a properties file
    public void loadFromPropertiesFile(String filePath) throws ConfigurationException {
        Configurations configurations = new Configurations();
        PropertiesConfiguration propertiesConfig = configurations.properties(new File(filePath));
        config.addConfiguration(propertiesConfig);
    }

    // Load configuration from environment variables
    public void loadFromEnvironment() {
        SystemConfiguration systemConfig = new SystemConfiguration();
        config.addConfiguration(systemConfig);
    }

    public void set(String key, String value) {
        config.setProperty(key, value);
    }

    // Get a configuration value
    public String get(String key) {
        return config.getString(key);
    }

    // Get a configuration value with a default
    public String getOrDefault(String key, String defaultValue) {
        return config.getString(key, defaultValue);
    }

    // List all configuration keys
    public void listAllKeys() {
        Iterator<String> keys = config.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            System.out.println(key + " = " + config.getString(key));
        }
    }

}
