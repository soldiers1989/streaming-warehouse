package com.tree.finance.bigdata.hive.streaming.tools.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/20 23:32
 */
public class HFileWriter extends RecordWriter<ImmutableBytesWritable, KeyValue>{

    public static Logger LOG = LoggerFactory.getLogger(HFileWriter.class);
    private final Map<byte[], WriterLength> writers;
    private byte[] previousRow;
    private final byte[] now;
    private boolean rollRequested;
    private Path outputPath;
    private FileSystem fs;
    private long maxsize;
    private Compression.Algorithm defaultCompression;
    Map<byte[], Compression.Algorithm> compressionMap;
    private Configuration conf;
    boolean compactionExclude;
    Map<byte[], BloomType> bloomTypeMap;
    DataBlockEncoding overriddenEncoding;
    Map<byte[], Integer> blockSizeMap;
    Map<byte[], DataBlockEncoding> datablockEncodingMap;

    public HFileWriter(Path output, Configuration conf) throws IOException{
        this.writers = new TreeMap(Bytes.BYTES_COMPARATOR);
        this.previousRow = HConstants.EMPTY_BYTE_ARRAY;
        this.now = Bytes.toBytes(System.currentTimeMillis());
        this.rollRequested = false;
        this.fs = FileSystem.get(conf);
        this.outputPath = output;
        this.maxsize = conf.getLong("hbase.hregion.max.filesize", 10737418240L);

        this.conf = conf;
        String defaultCompressionStr = conf.get("hfile.compression", Compression.Algorithm.NONE.getName());
        this.defaultCompression = AbstractHFileWriter.compressionByName(defaultCompressionStr);

        this.compactionExclude = conf.getBoolean("hbase.mapreduce.hfileoutputformat.compaction.exclude", false);
        this.compressionMap = createFamilyCompressionMap(conf);
        this.bloomTypeMap = createFamilyBloomTypeMap(conf);
        this.blockSizeMap = createFamilyBlockSizeMap(conf);
        String dataBlockEncodingStr = conf.get("hbase.mapreduce.hfileoutputformat.datablock.encoding");
        this.datablockEncodingMap = createFamilyDataBlockEncodingMap(conf);
        if (dataBlockEncodingStr != null) {
            overriddenEncoding = DataBlockEncoding.valueOf(dataBlockEncodingStr);
        } else {
            overriddenEncoding = null;
        }
    }

    public void write(ImmutableBytesWritable row, KeyValue kv) throws IOException {
        if (row == null && kv == null) {
            this.rollWriters();
        } else {
            byte[] rowKey = CellUtil.cloneRow(kv);
            long length = (long)kv.getLength();
            byte[] family = CellUtil.cloneFamily(kv);
            WriterLength wl = this.writers.get(family);
            if (wl == null) {
                fs.mkdirs(new Path(outputPath, Bytes.toString(family)));
            }
            if (wl != null && wl.written + length >= maxsize) {
                this.rollRequested = true;
            }
            if (this.rollRequested && Bytes.compareTo(this.previousRow, rowKey) != 0) {
                this.rollWriters();
            }
            if (wl == null || wl.writer == null) {
                wl = this.getNewWriter(family, conf);
            }
            kv.updateLatestStamp(this.now);
            wl.writer.append(kv);
            wl.written += length;
            this.previousRow = rowKey;
        }
    }

    private void rollWriters() throws IOException {
        WriterLength wl;
        for(Iterator i$ = this.writers.values().iterator(); i$.hasNext(); wl.written = 0L) {
            wl = (WriterLength)i$.next();
            if (wl.writer != null) {
                LOG.info("Writer=" + wl.writer.getPath() + (wl.written == 0L ? "" : ", wrote=" + wl.written));
                this.close(wl.writer);
            }

            wl.writer = null;
        }
        this.rollRequested = false;
    }

    private WriterLength getNewWriter(byte[] family, Configuration confx) throws IOException {
        WriterLength wl = new WriterLength();
        Path familydir = new Path(outputPath, Bytes.toString(family));
        Compression.Algorithm compression = compressionMap.get(family);
        compression = compression == null ? defaultCompression : compression;
        BloomType bloomType = bloomTypeMap.get(family);
        bloomType = bloomType == null ? BloomType.NONE : bloomType;
        Integer blockSize = blockSizeMap.get(family);
        blockSize = blockSize == null ? 65536 : blockSize;
        DataBlockEncoding encoding = overriddenEncoding;
        encoding = encoding == null ? datablockEncodingMap.get(family) : encoding;
        encoding = encoding == null ? DataBlockEncoding.NONE : encoding;
        Configuration tempConf = new Configuration(confx);
        tempConf.setFloat("hfile.block.cache.size", 0.0F);
        HFileContextBuilder contextBuilder = (new HFileContextBuilder()).withCompression(compression).withChecksumType(HStore.getChecksumType(confx)).withBytesPerCheckSum(HStore.getBytesPerChecksum(confx)).withBlockSize(blockSize);
        if (HFile.getFormatVersion(confx) >= 3) {
            contextBuilder.withIncludesTags(true);
        }

        contextBuilder.withDataBlockEncoding(encoding);
        HFileContext hFileContext = contextBuilder.build();
        wl.writer = (new StoreFile.WriterBuilder(confx, new CacheConfig(tempConf), fs)).withOutputDir(familydir).withBloomType(bloomType).withComparator(KeyValue.COMPARATOR).withFileContext(hFileContext).build();
        this.writers.put(family, wl);
        return wl;
    }
    private void close(StoreFile.Writer w) throws IOException {
        if (w != null) {
            w.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY, Bytes.toBytes(System.currentTimeMillis()));
            w.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY, Bytes.toBytes("single task"));
            w.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY, Bytes.toBytes(true));
            w.appendFileInfo(StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY, Bytes.toBytes(compactionExclude));
            w.appendTrackedTimestampsToMetadata();
            w.close();
        }
    }
    public void close(TaskAttemptContext c) throws IOException {
        Iterator iterator = this.writers.values().iterator();

        while(iterator.hasNext()) {
            WriterLength wl = (WriterLength)iterator.next();
            this.close(wl.writer);
        }

    }
    static Map<byte[], Compression.Algorithm> createFamilyCompressionMap(Configuration conf) {
        Map<byte[], String> stringMap = createFamilyConfValueMap(conf, "hbase.hfileoutputformat.families.compression");
        Map<byte[], Compression.Algorithm> compressionMap = new TreeMap(Bytes.BYTES_COMPARATOR);
        Iterator iterator = stringMap.entrySet().iterator();

        while(iterator.hasNext()) {
            Map.Entry<byte[], String> e = (Map.Entry)iterator.next();
            Compression.Algorithm algorithm = AbstractHFileWriter.compressionByName(e.getValue());
            compressionMap.put(e.getKey(), algorithm);
        }
        return compressionMap;
    }

    private static Map<byte[], String> createFamilyConfValueMap(Configuration conf, String confName) {
        Map<byte[], String> confValMap = new TreeMap(Bytes.BYTES_COMPARATOR);
        String confVal = conf.get(confName, "");
        String[] arr$ = confVal.split("&");
        int len$ = arr$.length;

        for(int i$ = 0; i$ < len$; ++i$) {
            String familyConf = arr$[i$];
            String[] familySplit = familyConf.split("=");
            if (familySplit.length == 2) {
                try {
                    confValMap.put(URLDecoder.decode(familySplit[0], "UTF-8").getBytes(), URLDecoder.decode(familySplit[1], "UTF-8"));
                } catch (UnsupportedEncodingException var10) {
                    throw new AssertionError(var10);
                }
            }
        }
        return confValMap;
    }

    static Map<byte[], BloomType> createFamilyBloomTypeMap(Configuration conf) {
        Map<byte[], String> stringMap = createFamilyConfValueMap(conf, "hbase.hfileoutputformat.families.bloomtype");
        Map<byte[], BloomType> bloomTypeMap = new TreeMap(Bytes.BYTES_COMPARATOR);
        Iterator i$ = stringMap.entrySet().iterator();

        while(i$.hasNext()) {
            Map.Entry<byte[], String> e = (Map.Entry)i$.next();
            BloomType bloomType = BloomType.valueOf((String)e.getValue());
            bloomTypeMap.put(e.getKey(), bloomType);
        }

        return bloomTypeMap;
    }
    static Map<byte[], Integer> createFamilyBlockSizeMap(Configuration conf) {
        Map<byte[], String> stringMap = createFamilyConfValueMap(conf, "hbase.mapreduce.hfileoutputformat.blocksize");
        Map<byte[], Integer> blockSizeMap = new TreeMap(Bytes.BYTES_COMPARATOR);
        Iterator i$ = stringMap.entrySet().iterator();

        while(i$.hasNext()) {
            Map.Entry<byte[], String> e = (Map.Entry)i$.next();
            Integer blockSize = Integer.parseInt((String)e.getValue());
            blockSizeMap.put(e.getKey(), blockSize);
        }
        return blockSizeMap;
    }
    static Map<byte[], DataBlockEncoding> createFamilyDataBlockEncodingMap(Configuration conf) {
        Map<byte[], String> stringMap = createFamilyConfValueMap(conf, "hbase.mapreduce.hfileoutputformat.families.datablock.encoding");
        Map<byte[], DataBlockEncoding> encoderMap = new TreeMap(Bytes.BYTES_COMPARATOR);
        Iterator iterator = stringMap.entrySet().iterator();

        while(iterator.hasNext()) {
            Map.Entry<byte[], String> e = (Map.Entry)iterator.next();
            encoderMap.put(e.getKey(), DataBlockEncoding.valueOf(e.getValue()));
        }
        return encoderMap;
    }
    static class WriterLength {
        long written = 0L;
        StoreFile.Writer writer = null;
        WriterLength() {
        }
    }

}
