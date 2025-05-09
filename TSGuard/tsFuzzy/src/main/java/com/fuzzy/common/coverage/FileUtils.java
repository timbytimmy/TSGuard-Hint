package com.fuzzy.common.coverage;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
public class FileUtils {

    public static void writeToFile(String content, String filePath) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write(content);
        }
    }

    public static List<String> getFilesWithSuffix(String directoryPath, String fileSuffix) throws IOException {
        List<String> filePaths = new ArrayList<>();
        Path dirPath = Paths.get(directoryPath);

        // Walk through the directory and filter files with the given suffix
        try (Stream<Path> stream = Files.walk(dirPath)) {
            stream.filter(p -> p.toString().endsWith(fileSuffix) && Files.isRegularFile(p))
                    .forEach(p -> filePaths.add(p.toString()));
        }
        return filePaths;
    }

    public static void createDir(String dirPath) {
        File directory = new File(dirPath);
        if (!directory.exists()) {
            if (directory.mkdirs()) {
                log.info("文件夹创建成功！");
            } else {
                log.info("文件夹创建失败！");
            }
        } else {
            log.info("文件夹已经存在。");
        }
    }

    // 将某个路径下的文件全部拷贝至另一个文件
    public static void copyFiles(Path sourceDir, Path targetDir) throws IOException {
        if (!Files.exists(targetDir)) {
            Files.createDirectories(targetDir);
        }

        Files.walkFileTree(sourceDir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path sourceFile, BasicFileAttributes attrs) throws IOException {
                Path targetFile = targetDir.resolve(sourceDir.relativize(sourceFile));
                if (!Files.exists(targetFile.getParent())) {
                    Files.createDirectories(targetFile.getParent());
                }
                Files.copy(sourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                return FileVisitResult.CONTINUE;
            }
        });
    }

    // 将某个文件拷贝至指定文件夹
    public static void copyFileToDirectory(Path sourceFile, Path targetDir) throws IOException {
        if (!Files.exists(targetDir)) {
            Files.createDirectories(targetDir);
        }
        Path targetFile = targetDir.resolve(sourceFile.getFileName());
        Files.copy(sourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
    }
}
