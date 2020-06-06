package com.sym.util;

import com.google.gson.Gson;

import java.util.Objects;

/**
 * @author shenyanming
 * @date 2020/6/6 21:44.
 */

public class JSONUtil {

    private static Gson gson = new Gson();

    public static String toJson(Object object){
        Objects.requireNonNull(object);
        return gson.toJson(object);
    }
}
