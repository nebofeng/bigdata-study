package com.nebo.homework.kafka.an2.utils;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public final class JSONUtil
{

  private static final Logger LOG = LoggerFactory.getLogger(JSONUtil.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectMapper NEWMAPPER = new ObjectMapper();
  private static JSONUtil jsonUtil;
  private static Gson GSON = new Gson();

  static {
    NEWMAPPER.setVisibility(JsonMethod.FIELD, JsonAutoDetect.Visibility.ANY);
    NEWMAPPER.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  private JSONUtil()
  {
  }

  public static JSONUtil getInstance()
  {
    synchronized (JSONUtil.class) {
      if (jsonUtil == null) {
        jsonUtil = new JSONUtil();
      }
    }

    return jsonUtil;
  }

  public static String fromObject(Object obj) throws IOException, JsonGenerationException, JsonMappingException
  {
    StringWriter stringWriter = new StringWriter();
    MAPPER.writeValue(stringWriter, obj);
    return stringWriter.toString();
  }

  public static String writeValueAsString(Object value)
  {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static String fromListForData(List<?> list) throws IOException, JsonGenerationException,
    JsonMappingException
  {
    StringWriter stringWriter = new StringWriter();
    stringWriter.write("{data:[");
    for (int i = 0; i < list.size(); i++) {
      stringWriter.write(fromObject(list.get(i)));
      if (i != list.size() - 1) {
        stringWriter.write(",");
      }
    }
    stringWriter.write("]}");
    return stringWriter.toString();
  }

  public static List<?> toList(String json) throws IOException, JsonGenerationException, JsonMappingException
  {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Get json string is:" + json);
    }
    MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    return MAPPER.readValue(json, List.class);
  }

  public static Map<?, ?> toMap(String json) throws IOException, JsonGenerationException, JsonMappingException
  {
    MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    return MAPPER.readValue(json, Map.class);
  }

  /**
   * 从Json字串中得到指定属性值
   *
   * @param jsonStr
   * @param proertyName
   * @return
   */
  public static Object getFromJson(String jsonStr, String proertyName)
  {
    Map map = new HashMap();
    try {
      map = JSONUtil.getInstance().toMap(jsonStr);
    } catch (Exception e) {
      LOG.error("", e);
    }
    return (Object) map.get(proertyName);
  }

  public static <T> T json2Object(String json, Class<T> clazz) throws JsonParseException, JsonMappingException, IOException
  {
    return MAPPER.readValue(json, clazz);
  }

  public static <T> T json2ObjectIgnoreDifference(String json, Class<T> clazz) throws JsonParseException, JsonMappingException, IOException
  {
    return NEWMAPPER.readValue(json, clazz);
  }

  public static String listToJsonStr(List<?> list, Type type)
  {
    //Type listType = new TypeToken<List<?>>(){}.getType();
    return GSON.toJson(list, type);
  }

  /**
   * Type listType = new TypeToken<List<T>>(){}.getType();
   */
  public static List<?> listFromJsonStr(String jsonStr, Type type)
  {
    List resultList = GSON.fromJson(jsonStr, type);

    return resultList;
  }
}
