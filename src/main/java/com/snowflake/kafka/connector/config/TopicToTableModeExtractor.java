package com.snowflake.kafka.connector.config;

import com.snowflake.kafka.connector.Utils;

public class TopicToTableModeExtractor {

  /** Defines whether single target table is fed by one or many source topics. */
  public enum Topic2TableMode {
    // Single topic = single table
    SINGLE_TOPIC_SINGLE_TABLE,
    // Multiple topics = single table
    MANY_TOPICS_SINGLE_TABLE,
  }

  private TopicToTableModeExtractor() {}

  /**
   * Util method - checks if given topic is defined in topic2Table map - if it is more than once, it
   * means multiple topics will store data in single table - in such case, for SNOWPIPE ingestion we
   * need to uniquely identify stage files so different instances of file cleaner won't handle
   * other's channel files.
   *
   * @param topic
   * @return
   */
  public static Topic2TableMode determineTopic2TableMode(
      TopicToTableConfig topicToTableConfig, String topic) {
    if (topicToTableConfig.useRegex()) {
      // assume many to one when using regex
      return Topic2TableMode.MANY_TOPICS_SINGLE_TABLE;
    }
    String tableName = Utils.tableName(topic, topicToTableConfig);
    return topicToTableConfig.getTopicToTableMap().values().stream()
                .filter(table -> table.equalsIgnoreCase(tableName))
                .count()
            > 1
        ? Topic2TableMode.MANY_TOPICS_SINGLE_TABLE
        : Topic2TableMode.SINGLE_TOPIC_SINGLE_TABLE;
  }
}
