/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.launcher;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

/**
 * Abstract Spark command builder that defines common functionality.
 */
abstract class AbstractCommandBuilder {

    boolean verbose;
    String appName;
    String appResource;
    String deployMode;
    String javaHome;
    String mainClass;
    String master;
    protected String propertiesFile;
    final List<String> appArgs;
    final List<String> jars;
    final List<String> files;
    final List<String> pyFiles;
    final Map<String, String> childEnv;
    final Map<String, String> conf;

    // The merged configuration for the application. Cached to avoid having to read / parse
    // properties files multiple times.
    private Map<String, String> effectiveConfig;

    AbstractCommandBuilder() {
        this.appArgs = new ArrayList<>();
        this.childEnv = new HashMap<>();
        this.conf = new HashMap<>();
        this.files = new ArrayList<>();
        this.jars = new ArrayList<>();
        this.pyFiles = new ArrayList<>();
    }

    /**
     * Builds the command to execute.
     *
     * @param env A map containing environment variables for the child process. It may already contain
     *            entries defined by the user (such as SPARK_HOME, or those defined by the
     *            SparkLauncher constructor that takes an environment), and may be modified to
     *            include other variables needed by the process to be executed.
     */
    abstract List<String> buildCommand(Map<String, String> env)
            throws IOException, IllegalArgumentException;

    /**
     * Builds a list of arguments to run java.
     *
     * This method finds the java executable to use and appends JVM-specific options for running a
     * class with Spark in the classpath. It also loads options from the "java-opts" file in the
     * configuration directory being used.
     *
     * Callers should still add at least the class to run, as well as any arguments to pass to the
     * class.
     */
    List<String> buildJavaCommand(String extraClassPath) throws IOException {
        List<String> cmd = new ArrayList<>();
        String envJavaHome;

        if (javaHome != null) {
            cmd.add(join(File.separator, javaHome, "bin", "java"));
        } else if ((envJavaHome = System.getenv("JAVA_HOME")) != null) {
            cmd.add(join(File.separator, envJavaHome, "bin", "java"));
        } else {
            cmd.add(join(File.separator, System.getProperty("java.home"), "bin", "java"));
        }

        // Load extra JAVA_OPTS from conf/java-opts, if it exists.
        File javaOpts = new File(join(File.separator, getConfDir(), "java-opts"));
        if (javaOpts.isFile()) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(
                    new FileInputStream(javaOpts), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    addOptionString(cmd, line);
                }
            }
        }

        cmd.add("-cp");
        cmd.add(join(File.pathSeparator, buildClassPath(extraClassPath)));
        return cmd;
    }

    void addOptionString(List<String> cmd, String options) {
        if (!isEmpty(options)) {
            for (String opt : parseOptionString(options)) {
                cmd.add(opt);
            }
        }
    }

    /**
     * Builds the classpath for the application. Returns a list with one classpath entry per element;
     * each entry is formatted in the way expected by <i>java.net.URLClassLoader</i> (more
     * specifically, with trailing slashes for directories).
     */
    List<String> buildClassPath(String appClassPath) throws IOException {

        // 重定向使用classpath,而非dev 环境下
        String classz = System.getProperty("java.class.path");
        List<String> strings = Arrays.asList(classz.split(";"));
        return strings;
    }

    /**
     * Adds entries to the classpath.
     *
     * @param cp List to which the new entries are appended.
     * @param entries New classpath entries (separated by File.pathSeparator).
     */
    private void addToClassPath(Set<String> cp, String entries) {
        if (isEmpty(entries)) {
            return;
        }
        String[] split = entries.split(Pattern.quote(File.pathSeparator));
        for (String entry : split) {
            if (!isEmpty(entry)) {
                if (new File(entry).isDirectory() && !entry.endsWith(File.separator)) {
                    entry += File.separator;
                }
                cp.add(entry);
            }
        }
    }

    String getScalaVersion() {
            return "2.12";

    }

    String getSparkHome() {
        String path = getenv(ENV_SPARK_HOME);
        if (path == null && "1".equals(getenv("SPARK_TESTING"))) {
            path = System.getProperty("spark.test.home");
        }
        checkState(path != null,
                "Spark home not found; set it explicitly or use the SPARK_HOME environment variable.");
        return path;
    }

    String getenv(String key) {
        return firstNonEmpty(childEnv.get(key), System.getenv(key));
    }

    void setPropertiesFile(String path) {
        effectiveConfig = null;
        this.propertiesFile = path;
    }

    Map<String, String> getEffectiveConfig() throws IOException {
        if (effectiveConfig == null) {
            effectiveConfig = new HashMap<>(conf);
            Properties p = loadPropertiesFile();
            for (String key : p.stringPropertyNames()) {
                if (!effectiveConfig.containsKey(key)) {
                    effectiveConfig.put(key, p.getProperty(key));
                }
            }
        }
        return effectiveConfig;
    }

    /**
     * Loads the configuration file for the application, if it exists. This is either the
     * user-specified properties file, or the spark-defaults.conf file under the Spark configuration
     * directory.
     */
    private Properties loadPropertiesFile() throws IOException {
        Properties props = new Properties();
        File propsFile;
        if (propertiesFile != null) {
            propsFile = new File(propertiesFile);
            checkArgument(propsFile.isFile(), "Invalid properties file '%s'.", propertiesFile);
        } else {
            propsFile = new File(getConfDir(), DEFAULT_PROPERTIES_FILE);
        }

        if (propsFile.isFile()) {
            try (InputStreamReader isr = new InputStreamReader(
                    new FileInputStream(propsFile), StandardCharsets.UTF_8)) {
                props.load(isr);
                for (Map.Entry<Object, Object> e : props.entrySet()) {
                    e.setValue(e.getValue().toString().trim());
                }
            }
        }
        return props;
    }

    private String getConfDir() {
        String confDir = getenv("SPARK_CONF_DIR");
        return confDir != null ? confDir : join(File.separator, getSparkHome(), "conf");
    }

}
