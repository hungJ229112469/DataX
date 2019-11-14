package com.alibaba.datax.utils;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.TransformerErrorCode;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author HL
 * @date 2019/10/9 10:36
 */
public class MappingUtils {
    private static Map<Integer, Map<String, String>> valueMap = new ConcurrentHashMap<Integer, Map<String, String>>();

    private static Map<Integer, String> urlMap = new ConcurrentHashMap<Integer, String>();


    public static String replace(int index, String value) {
        if (!valueMap.containsKey(index)) {
            getValueMap(index);
        }
        Map<String, String> map = valueMap.get(index);
        if (map != null) {
            return map.get(value);
        } else {
            return value;
        }
    }

    public static void putUrl(int index, String url) {
        String s = urlMap.get(index);
        if (StringUtils.isBlank(s)) {
            urlMap.put(index, url);
        } else {
            if (!s.equals(url)) {
                throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_CONFIGURATION_ERROR, "获取映射配置接口异常");
            }
        }
    }

    private static synchronized void getValueMap(int index) {
        if (valueMap.containsKey(index)) {
            return;
        }
        String urlStr = urlMap.get(index);
        Map<String, String> map = null;
        HttpURLConnection httpURLConnection = null;
        InputStream inputStream = null;
        try {
            URL url = new URL(urlStr);
            httpURLConnection = ((HttpURLConnection) url.openConnection());
            httpURLConnection.setRequestMethod("GET");
            httpURLConnection.setConnectTimeout(5000);
            httpURLConnection.setReadTimeout(5000);
            httpURLConnection.connect();
            int responseCode = httpURLConnection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                inputStream = httpURLConnection.getInputStream();
                String result = isToString(inputStream);
                if (StringUtils.isNotBlank(result)) {
                    map = JSON.parseObject(result, Map.class);
                }
            }
        } catch (Exception e) {
            DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_CONFIGURATION_ERROR, "获取映射配置接口异常", e);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (httpURLConnection != null) {
                httpURLConnection.disconnect();
            }
        }
        System.out.println("映射关系=======" + map.toString());
        valueMap.put(index, map);
    }

    private static String isToString(InputStream inputStream) {
        StringBuffer result = new StringBuffer();
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result.toString();
    }
}
