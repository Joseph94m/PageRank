package mapreduce;

/*
 https://stackoverflow.com/questions/13364153/awk-one-liner-select-only-rows-based-on-value-of-a-column
 */

import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MultiLineInputFormat extends NLineInputFormat{
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit genericSplit, TaskAttemptContext context) {
        context.setStatus(genericSplit.toString());
        return new MultiLineRecordReader();
    }

    public static class MultiLineRecordReader extends RecordReader<LongWritable, Text>{
        private int NLINESTOPROCESS;
        private LineReader in;
        private LongWritable key;
        private Text value = new Text();
        private long start =0;
        private long end =0;
        private long pos =0;
        private int maxLineLength;

        @Override
        public void close() throws IOException {
            if (in != null) {
                in.close();
            }
        }

        @Override
        public LongWritable getCurrentKey() throws IOException,InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            if (start == end) {
                return 0.0f;
            }
            else {
                return Math.min(1.0f, (pos - start) / (float)(end - start));
            }
        }
        /*
         * 
         * NLINESTOPROCESS gets the number of lines specified in the PageRank run function.
         * start specifies the offset at which to begin to read for this call.
         * end specifies the offset at which to stop to read for this call.
         * 
         */
        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context)throws IOException, InterruptedException {
            NLINESTOPROCESS = getNumLinesPerSplit(context);
            FileSplit split = (FileSplit) genericSplit;
            final Path file = split.getPath();
            Configuration conf = context.getConfiguration();
            this.maxLineLength = conf.getInt("mapreduce.input.linerecordreader.line.maxlength",Integer.MAX_VALUE);
            FileSystem fs = file.getFileSystem(conf);
            start = split.getStart();
            end= start + split.getLength();
            boolean skipFirstLine = false;
            FSDataInputStream filein = fs.open(split.getPath());

            if (start != 0){
                skipFirstLine = true;
                --start;
                filein.seek(start);
            }
            in = new LineReader(filein,conf);
            if(skipFirstLine){
                start += in.readLine(new Text(),0,(int)Math.min((long)Integer.MAX_VALUE, end - start));
            }
            this.pos = start;
        }

        /*
         * 
         * 
         * For each call, read line by line NLINESTOPROCESS times. For each line, read whole content and append to value as a newline.
         * 
         */
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (key == null) {
                key = new LongWritable();
            }
            key.set(pos);
            if (value == null) {
                value = new Text();
            }
            value.clear();
            final Text endline = new Text("\n");
            int newSize = 0;
            for(int i=0;i<NLINESTOPROCESS;i++){
                Text v = new Text();
                while (pos < end) {
                    newSize = in.readLine(v, maxLineLength,Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),maxLineLength));
                    value.append(v.getBytes(),0, v.getLength());
                    value.append(endline.getBytes(),0, endline.getLength());
                    if (newSize == 0) {
                        break;
                    }
                    pos += newSize;
                    if (newSize < maxLineLength) {
                        break;
                    }
                }
            }
            if (newSize == 0) {
                key = null;
                value = null;
                return false;
            } else {
                return true;
            }
        }
    }

}