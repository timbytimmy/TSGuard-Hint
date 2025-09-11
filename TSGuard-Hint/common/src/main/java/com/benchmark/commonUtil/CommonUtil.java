package com.benchmark.commonUtil;

import com.benchmark.constants.DataType;
import com.benchmark.dto.DBValParam;
import com.benchmark.entity.DBVal;
import com.benchmark.entity.PointData;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;

public class CommonUtil {
    /**
     * 将标准时间转为Unix秒
     *
     * @param date String格式时间
     * @return long
     * @author none
     * @date 2020-11-05 00:00
     */
    public static long dateStringToUTCSeconds(String date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return (long) (format.parse(date).getTime() / 1000);
    }

    /**
     * 将标准时间转为Unix毫秒
     *
     * @param date String格式时间
     * @return long
     * @author none
     * @date 2020-11-05 00:00
     */
    public static long dateStringToUTCMilliSeconds(String date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.parse(date).getTime();
    }


    /**
     * 将Unix秒转为标准时间
     *
     * @param utcSeconds long格式当前unix时间秒数
     * @return String
     * @author none
     * @date 2020-11-05 00:00
     */
    public static String uTCSecondsToDateString(long utcSeconds) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.format(new Date(utcSeconds * 1000));
    }

    /**
     * 将Unix秒转为标准时间
     *
     * @param utcMilliSeconds long格式当前unix时间毫秒数
     * @return String
     * @author none
     * @date 2020-11-05 00:00
     */
    public static String uTCMilliSecondsToDateString(long utcMilliSeconds) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return format.format(new Date(utcMilliSeconds));
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    /**
     * 将Unix秒转为标准时间
     *
     * @param utcMilliSeconds long格式当前unix时间毫秒数
     * @return String
     * @author none
     * @date 2020-11-05 00:00
     */
    public static String uTCMilliSecondsToDateStringWithMs(long utcMilliSeconds) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
            return format.format(new Date(utcMilliSeconds));
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    public static String uTCMSToDateString(String utcSeconds) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long second = Long.parseLong(utcSeconds.substring(0, 10));
        return (format.format(new Date(second * 1000))) + ":" + utcSeconds.substring(10);
    }

    /**
     * 将Unix秒转为标准时间
     *
     * @param utcSeconds long格式当前unix时间秒数
     * @return String
     * @author none
     * @date 2020-11-05 00:00
     */
    public static String uTCSecondsToDateStringYear(long utcSeconds) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy");
        return format.format(new Date(utcSeconds * 1000));
    }

    /**
     * 将Unix秒转为标准时间
     *
     * @param utcSeconds long格式当前unix时间秒数
     * @return String
     * @author none
     * @date 2020-11-05 00:00
     */
    public static String uTCSecondsToDateStringMonth(long utcSeconds) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("MM");
        return format.format(new Date(utcSeconds * 1000));
    }

    /**
     * 返回当前时间的Unix值，单位为毫秒
     *
     * @return long
     * @author none
     * @date 2020-11-05 00:00
     */
    public static long currentUTCSeconds() {
        return System.currentTimeMillis() / 1000;
    }

    /**
     * 返回当前时间的Unix值，单位为毫秒
     *
     * @return long
     * @author none
     * @date 2020-11-05 00:00
     */
    public static long currentUTCMilliSeconds() {
        return System.currentTimeMillis();
    }

    public static OffsetDateTime toOffsetDateTime(long timeMilli) {
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(timeMilli), ZoneId.systemDefault());
    }

    /**
     * @param timestamp
     * @return java.sql.Timestamp
     * @description 将Java长整型时间戳转为Mysql支持时间戳类型(精度为ms)
     * @dateTime 2023/3/4 9:13
     */
    public static Timestamp milliLongToSqlTimestamp(long timestamp) {
        return new Timestamp(timestamp);
    }

    /**
     * 返回human-readable的数据尺寸大小
     *
     * @param size
     * @return String
     * @author none
     * @date 2020-11-05 00:00
     */
    public static String getNetFileSizeDescription(long size) {
        StringBuffer bytes = new StringBuffer();
        DecimalFormat format = new DecimalFormat("###.0");
        if (size >= 1024 * 1024 * 1024) {
            double i = (size / (1024.0 * 1024.0 * 1024.0));
            bytes.append(format.format(i)).append("GB");
        } else if (size >= 1024 * 1024) {
            double i = (size / (1024.0 * 1024.0));
            bytes.append(format.format(i)).append("MB");
        } else if (size >= 1024) {
            double i = (size / (1024.0));
            bytes.append(format.format(i)).append("KB");
        } else if (size < 1024) {
            if (size <= 0) {
                bytes.append("0B");
            } else {
                bytes.append((int) size).append("B");
            }
        }
        return bytes.toString();
    }

    /*
    判断对象是否为空，进一步判断对象中的属性是否都为空
     */
    public boolean objCheckIsNull(Object object) {
        Class clazz = (Class) object.getClass(); // 得到类对象
        Field fields[] = clazz.getDeclaredFields(); // 得到所有属性
        boolean flag = true; //定义返回结果，默认为true
        for (Field field : fields) {
            field.setAccessible(true);
            Object fieldValue = null;
            try {
                fieldValue = field.get(object); //得到属性值
                Type fieldType = field.getGenericType();//得到属性类型
                String fieldName = field.getName(); // 得到属性名
                System.out.println("属性类型：" + fieldType + ",属性名：" + fieldName + ",属性值：" + fieldValue);
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            if (fieldValue != null) {  //只要有一个属性值不为null 就返回false 表示对象不为null
                flag = false;
                break;
            }
        }
        return flag;
    }

    /**
     * @param jsonDirectoryPath
     * @return java.util.List<java.lang.String>
     * @description 指定JSON目录，读取该目录下所有JSON文件
     * @dateTime 2023/1/13 19:35
     */
    public static List<String> getJSONFile(String jsonDirectoryPath) {
        List<String> filePaths = new ArrayList<>();
        if (jsonDirectoryPath.isEmpty()) {
            return filePaths;
        }

        File file = new File(jsonDirectoryPath);
        File[] fileArray = file.listFiles();

        for (File f : fileArray) {
            if (f.isFile() && f.getName().contains(".json")) {
                filePaths.add(f.getPath());
            } else if (f.isDirectory()) {
                getJSONFile(f.getPath());
            }
        }

        return filePaths;
    }

    // TODO 目前仅支持单tagName形式，后续需改动

    /**
     * @param dbValParams
     * @return java.util.Hashtable<com.sun.tools.javac.util.Pair < java.lang.String, java.lang.String>,java.util.List<db.DBVal>>
     * @description 将无序DBValParam列表按tableName和tagName分组存放，Pair<tableName, tagName>
     * @dateTime 2023/2/15 20:30
     */
    public static Hashtable<Pair<String, String>, List<DBValParam>> groupDBValParamByTableNameAndTagName(
            List<DBValParam> dbValParams) {
        Hashtable<Pair<String, String>, List<DBValParam>> result = new Hashtable<>();

        for (DBValParam dbValParam : dbValParams) {
            Pair<String, String> pair = new ImmutablePair<>(dbValParam.getTableName(), dbValParam.getTagName());

            if (result.containsKey(pair)) {
                result.get(pair).add(dbValParam);
            } else {
                List<DBValParam> dbVals = new ArrayList<>();
                dbVals.add(dbValParam);
                result.put(pair, dbVals);
            }
        }

        return result;
    }

    /**
     * @param dbValList
     * @return java.util.Hashtable<com.sun.tools.javac.util.Pair < java.lang.String, java.lang.String>,java.util.List<db.DBVal>>
     * @description 将无序DBVal列表按tableName和tagName分组存放，Pair<tableName, tagName>
     * @dateTime 2023/2/15 20:30
     */
    public static Hashtable<Pair<String, String>, List<DBVal>> groupDBValByTableNameAndTagName(List<DBVal> dbValList) {
        Hashtable<Pair<String, String>, List<DBVal>> result = new Hashtable<>();

        for (DBVal dbVal : dbValList) {
            Pair<String, String> pair = new ImmutablePair<>(dbVal.getTableName(), dbVal.getTagName());

            if (result.containsKey(pair)) {
                result.get(pair).add(dbVal);
            } else {
                List<DBVal> dbVals = new ArrayList<>();
                dbVals.add(dbVal);
                result.put(pair, dbVals);
            }
        }

        return result;
    }

    /**
     * @param dbValList
     * @return java.util.Hashtable<java.lang.String, java.util.List < com.benchmark.entity.DBVal>>
     * @description 将dbVal列表按照tagValue分组
     * @dateTime 2023/3/3 9:12
     */
    public static Hashtable<String, List<DBVal>> groupDBValByTagValue(List<DBVal> dbValList) {
        Hashtable<String, List<DBVal>> result = new Hashtable<>();

        for (DBVal dbVal : dbValList) {
            String tagValue = dbVal.getTagValue();

            if (result.containsKey(tagValue)) {
                result.get(tagValue).add(dbVal);
            } else {
                List<DBVal> dbVals = new ArrayList<>();
                dbVals.add(dbVal);
                result.put(tagValue, dbVals);
            }
        }

        return result;
    }

    /**
     * @param dbValList
     * @return java.util.List<db.PointData>
     * @description 将DBVals转换为PointDatas
     * @dateTime 2023/2/4 10:43
     */
    @NonNull
    public static List<PointData> convertDBValsToPointData(@NonNull List<DBVal> dbValList) {
        List<PointData> results = new ArrayList<>();
        if (dbValList.isEmpty()) {
            return results;
        }

        dbValList.forEach(dbVal -> {
            String tableName = dbVal.getTableName();
            String tagName = dbVal.getTagName();
            String tagValue = dbVal.getTagValue();
            long utcTime = dbVal.getUtcTimeMilliSeconds();
            List<String> fieldNames = dbVal.getFieldNames();
            List<DataType> fieldTypes = dbVal.getFieldTypes();
            List<Object> fieldValues = dbVal.getFieldValues();
            // 若干字段
            for (int i = 0; i < dbVal.getFieldSize(); i++) {
                String fieldName = fieldNames.get(i);
                DataType fieldType = fieldTypes.get(i);
                Object fieldValue = fieldValues.get(i);

                PointData pointData = PointData.PointDataBuilder.anPointData()
                        .withTableName(tableName)
                        .withTagName(tagName)
                        .withTagValue(tagValue)
                        .withUtcTime(utcTime)
                        .withFieldName(fieldName)
                        .withFieldValue(fieldValue)
                        .withFieldType(fieldType)
                        .build();
                results.add(pointData);
            }
        });

        return results;
    }
}
