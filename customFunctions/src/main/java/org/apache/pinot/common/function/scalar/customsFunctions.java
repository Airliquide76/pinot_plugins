package org.apache.pinot.common.function.scalar;
import javax.annotation.Nullable;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.JsonUtils;

import static org.apache.calcite.runtime.SqlFunctions.not;
import static org.apache.pinot.common.function.scalar.JsonFunctions.jsonPath;


public class customsFunctions {
  @ScalarFunction(nullableParameters = true)
  public static String jsonPathStringFillNA(@Nullable Object object, String jsonPath, String defaultValue) {
    try {
      Object jsonValue = jsonPath(object, jsonPath);
      if ((jsonValue instanceof String) && !(String.valueOf(jsonValue).isBlank())) {
        return (String) jsonValue;
      }
      return jsonValue == null || String.valueOf(jsonValue).isBlank()
          ? defaultValue : JsonUtils.objectToString(jsonValue);
    } catch (Exception ignore) {
      return defaultValue;
    }
  }
}
