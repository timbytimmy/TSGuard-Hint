package com.fuzzy.prometheus.client.builder;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    private final static Pattern namedFormatPattern = Pattern.compile("#\\{(?<key>.*?)}");
    public static String namedFormat(final String format, Map<String, ? extends Object> kvs) {
        final StringBuffer buffer = new StringBuffer();
        final Matcher match = namedFormatPattern.matcher(format);
        while (match.find()) {
            final String key = match.group("key");
            final Object value = kvs.get(key);
            if (value != null)
                match.appendReplacement(buffer, value.toString());
            else if (kvs.containsKey(key))
                match.appendReplacement(buffer, "null");
            else
                match.appendReplacement(buffer, "");
        }
        match.appendTail(buffer);
        return buffer.toString();
    }    
}
