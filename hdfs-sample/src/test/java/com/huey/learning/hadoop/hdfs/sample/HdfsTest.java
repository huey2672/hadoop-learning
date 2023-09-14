package com.huey.learning.hadoop.hdfs.sample;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 */
public class HdfsTest {

    private static Configuration conf;

    @BeforeClass
    public static void init() {
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
    }

    @Test
    public void testMkdirs() throws IOException {
        try (FileSystem fs = FileSystem.get(conf)) {
            Path directory = new Path("/java-examples/");
            fs.mkdirs(directory);
        }
    }

    @Test
    public void testCopyFromLocalFile() throws IOException {
        try (FileSystem fs = FileSystem.get(conf)) {
            Path srcFile = new Path("/etc/hosts");
            Path dstDirectory = new Path("/java-examples/");
            fs.copyFromLocalFile(srcFile, dstDirectory);
        }
    }

    @Test
    public void testCopyToLocalFile() throws IOException {
        try (FileSystem fs = FileSystem.get(conf)) {
            Path srcFile = new Path("/java-examples/hosts");
            Path dstDirectory = new Path("/tmp/");
            fs.copyToLocalFile(srcFile, dstDirectory);
        }
    }

    @Test
    public void testDeleteFile() throws IOException {
        try (FileSystem fs = FileSystem.get(conf)) {
            Path directory = new Path("/java-examples/hosts");
            fs.delete(directory, false);
        }
    }

    @Test
    public void testDeleteDirectory() throws IOException {
        try (FileSystem fs = FileSystem.get(conf)) {
            Path file = new Path("/java-examples/");
            fs.delete(file, true);
        }
    }

    @Test
    public void testUpload() throws IOException {
        try (InputStream inputStream = new FileInputStream("/etc/hosts");
                FileSystem fs = FileSystem.get(conf);
                OutputStream outputStream = fs.create(new Path("/java-examples/hosts"))) {
            IOUtils.copy(inputStream, outputStream);
        }
    }

    @Test
    public void testDownload() throws IOException {
        try (FileSystem fs = FileSystem.get(conf);
                InputStream inputStream = fs.open(new Path("/java-examples/hosts"));
                OutputStream outputStream = new FileOutputStream("/tmp/hosts")) {
            IOUtils.copy(inputStream, outputStream);
        }
    }

}
