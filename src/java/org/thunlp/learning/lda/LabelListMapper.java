package org.thunlp.learning.lda;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.thunlp.misc.Counter;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

public class LabelListMapper implements Mapper<Text, Text, Text, LongWritable> {
    public final static String FIELD_SEP = "\t";
    public final static int FIELD_COUNT = 2;
    public static String NUM_DOCS_STRING = " ";
    Text outkey = new Text();
    LongWritable outvalue = new LongWritable(1);

    public void configure(JobConf job) {
    }

    public void map(Text key, Text value, OutputCollector<Text, LongWritable> output,
                    Reporter r) throws IOException {
        String[] fields = value.toString().split(FIELD_SEP, FIELD_COUNT);
        if(fields.length < FIELD_COUNT) {
            throw new IOException("Not enough fields found in value");
        }
        String text = fields[0];
        String[] labels = text.split(",");
        for (String l : labels) {
            l = l.replace("[", "");
            l = l.replace("]", "");
            l = l.replace("\"", "");
            outkey.set(l);
            output.collect(outkey, outvalue);
        }
    }

    public void close() {
    }
}


