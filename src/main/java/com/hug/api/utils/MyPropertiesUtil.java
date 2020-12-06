package com.hug.api.utils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * @ClassName: MyPropertiesUtil
 * @Description:
 * @Author: hug on 2020/10/24 9:42 (星期六)
 * @Version: 1.0
 */
public class MyPropertiesUtil {
    public static Properties getProperties(String path) {
        Properties properties = new Properties();
        try {
            properties.load(new InputStreamReader(MyPropertiesUtil.class.getClassLoader().getResourceAsStream(path)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

}
