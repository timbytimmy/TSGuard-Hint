package com.tsFuzzy.tsdbms;

public class TestUtil {

//    public static void setLogFile(String filePath) {
//        // 获取 Logback 的 LoggerContext
//        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
//
//        // 创建文件输出 Appender
//        FileAppender fileAppender = new FileAppender();
//        fileAppender.setContext(context);
//        fileAppender.setName("FILE");
//        fileAppender.setFile(filePath);
//
//        // 设置日志格式
//        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
//        encoder.setContext(context);
//        encoder.setPattern("%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n");
//        encoder.start();
//
//        // 将 Encoder 绑定到 FileAppender
//        fileAppender.setEncoder(encoder);
//        fileAppender.start();
//
//        // 获取根日志器并绑定新的 Appender
//        Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
//        rootLogger.detachAndStopAllAppenders(); // 移除默认 Appender
//        rootLogger.addAppender(fileAppender);
//    }

}
