package com.datawarehouse;

import com.hadoop.compression.lzo.LzopCodec;
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;
import java.net.URI;

public class LzoSequenceFileWriter extends SequenceFileWriter {

    private String path;
    public LzoSequenceFileWriter(String path){
        this.path=path;
        Path destPath=new Path(path);
        FileSystem fileSystem = initFileSystem();
        try {
            super.open(fileSystem,destPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public LzoSequenceFileWriter(){
        new SequenceFileWriter(LzopCodec.class.getCanonicalName(), SequenceFile.CompressionType.BLOCK);
    }

    private FileSystem initFileSystem(){
        FileSystem fileSystem=null;
        try {
            Configuration conf=new Configuration();
            conf.setClass("mapreduce.map.output.compress.codec", LzopCodec.class, CompressionCodec.class);
            conf.set("mapreduce.map.output.compress", "true");
            fileSystem = FileSystem.get(new URI("hdfs://home0:9000"), conf, "root");
        }catch (Exception e){
            e.printStackTrace();
        }
        return fileSystem;
    }

}
