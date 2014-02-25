package tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

public class HBaseSequenceFileReader {

    private static Configuration conf = HBaseConfiguration.create();

    /**
     * @param args
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException {
        FileSystem fs = FileSystem.get(conf);
        String fileName = "/tmp/testSequenceFile";

        convertToPuts(fs, fileName);
    }

    private static void convertToPuts(FileSystem fs, String fileName) throws IOException {
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(fileName), conf);
        ImmutableBytesWritable key = new ImmutableBytesWritable();
        Result val = new Result();

        while (reader.next(key, val)) {
            Put p = resultToPut(key, val);
            // TODO: convert and put to hbase
        }
    }

    private static Put resultToPut(ImmutableBytesWritable key, Result result)
            throws IOException {
        Put put = new Put(key.get());
        for (KeyValue kv : result.raw()) {
            put.add(kv);
        }
        return put;
    }

}
