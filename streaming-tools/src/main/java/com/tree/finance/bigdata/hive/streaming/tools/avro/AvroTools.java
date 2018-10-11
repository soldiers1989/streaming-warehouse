package com.tree.finance.bigdata.hive.streaming.tools.avro;


import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AvroTools {

    public static void main(String[] args) throws Exception{
        AvroCatParser parser = new AvroCatParser(args);
        try {
            parser.init();
        }catch (ParseException e) {
            parser.printHelp();
            System.exit(1);
        }
        if (parser.isHelp()) {
            parser.printHelp();
        }
        String[] path = parser.getPath();
        catAvro(path);
    }

    public static void catAvro(String[] paths) throws Exception{
        FileSystem fs = FileSystem.get(new Configuration());
        for (String pathStr : paths) {
            Path path = new Path(pathStr);
            if (!fs.exists(path)) {
                System.out.println("path not exist: " + path);
                fs.close();
                System.exit(1);
            }
            if (fs.isDirectory(path)) {
                FileStatus[] statuses = fs.listStatus(path);
                String[] subPaths = new String[statuses.length];
                int i = 0;
                for (FileStatus status : statuses) {
                    subPaths[i++] = status.getPath().toString();
                }
                catAvro(subPaths);
            }
            if (fs.isFile(path)) {
                catAvroFile(path);
            }
        }
    }

    private static void catAvroFile(Path path) throws Exception{
        try (DataFileReader reader = new DataFileReader(new FsInput(path, new Configuration()), new GenericDatumReader());){
            while (reader.hasNext()) {
                System.out.println(reader.next().toString());
            }
        }
    }

}
