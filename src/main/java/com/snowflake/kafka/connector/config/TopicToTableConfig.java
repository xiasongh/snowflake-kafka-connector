package com.snowflake.kafka.connector.config;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class TopicToTableConfig {

  private final Map<String, String> topicToTableMap;
  private final Pattern topicToTableRegex;
  private final String topicToTableReplacement;
  private final boolean useHash;

  public TopicToTableConfig() {
    this(new HashMap<>(), null, null, true);
  }

  public TopicToTableConfig(Map<String, String> topicToTableMap) {
    this(topicToTableMap, null, null, true);
  }

  public TopicToTableConfig(
      Map<String, String> topicToTableMap,
      Pattern topicToTableRegex,
      String topicToTableReplacement,
      boolean useHash) {
    this.topicToTableMap = topicToTableMap;
    this.topicToTableRegex = topicToTableRegex;
    this.topicToTableReplacement = topicToTableReplacement;
    this.useHash = useHash;
  }

  public Map<String, String> getTopicToTableMap() {
    return this.topicToTableMap;
  }

  public Pattern getTopicToTableRegex() {
    return this.topicToTableRegex;
  }

  public String getTopicToTableReplacement() {
    return this.topicToTableReplacement;
  }

  public boolean useRegex() {
    return this.topicToTableRegex != null && this.topicToTableReplacement != null;
  }

  public boolean useHash() {
    return this.useHash;
  }

  public static TopicToTableConfig fromConfig(Map<String, String> config) {
    Map<String, String> topic2table = new HashMap<>();
    if (config.containsKey(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP)) {
      Map<String, String> result =
          Utils.parseTopicToTableMap(config.get(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP));
      if (result != null) {
        topic2table = result;
      }
    }

    boolean useHash = true;
    if (config.containsKey(SnowflakeSinkConnectorConfig.TOPICS_TABLES_HASH)) {
      useHash = Boolean.parseBoolean(config.get(SnowflakeSinkConnectorConfig.TOPICS_TABLES_HASH));
    }

    String regex = config.get(SnowflakeSinkConnectorConfig.TOPICS_TABLES_REGEX);
    String replacement = config.get(SnowflakeSinkConnectorConfig.TOPICS_TABLES_REPLACEMENT);
    if (regex != null && replacement != null) {
      return new TopicToTableConfig(topic2table, Pattern.compile(regex), replacement, useHash);
    } else {
      return new TopicToTableConfig(topic2table, null, null, useHash);
    }
  }
}
