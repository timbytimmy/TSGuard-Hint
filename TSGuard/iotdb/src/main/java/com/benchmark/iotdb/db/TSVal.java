package com.benchmark.iotdb.db;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class TSVal {
    private String path;
    private TSDataType dataType;
    private TSEncoding encoding;
    private CompressionType compressor;
}
