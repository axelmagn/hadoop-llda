package org.thunlp.learning.lda;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class LabelListReducer
        implements Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable outvalue = new LongWritable();

    public void reduce(Text key, Iterator<LongWritable> values,
                       OutputCollector<Text, LongWritable> output, Reporter r) throws IOException {
        long lf = 0;
        while (values.hasNext()) {
            lf += values.next().get();
        }
        outvalue.set(lf);
        output.collect(key, outvalue);
    }

    public void configure(JobConf conf) {
    }

    public void close() throws IOException {
    }

}
