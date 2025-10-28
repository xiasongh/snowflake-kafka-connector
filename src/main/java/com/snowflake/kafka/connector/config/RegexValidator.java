package com.snowflake.kafka.connector.config;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class RegexValidator implements ConfigDef.Validator {
  public void ensureValid(String name, Object value) {
    String s = (String) value;
    if (s == null) {
      return;
    }

    try {
      Pattern.compile(s);
    } catch (PatternSyntaxException e) {
      throw new ConfigException(name, s, "Invalid regex: " + e.getMessage());
    }
  }
}
