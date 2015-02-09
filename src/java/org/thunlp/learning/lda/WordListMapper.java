package org.thunlp.learning.lda;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.thunlp.misc.Counter;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

public class WordListMapper implements Mapper<Text, Text, Text, Text> {
    public final static String FIELD_SEP = "\t";
    public final static int FIELD_COUNT = 2;
    public static String NUM_DOCS_STRING = " ";
    Text outkey = new Text();
    Text outvalue = new Text();
    Counter<String> wordfreq = new Counter<String>();

    public void configure(JobConf job) {
    }

    public void map(Text key, Text value, OutputCollector<Text, Text> output,
                    Reporter r) throws IOException {
        String[] fields = value.toString().split(FIELD_SEP, FIELD_COUNT);
        if(fields.length < FIELD_COUNT) {
            throw new IOException("Not enough fields found in value");
        }
        String text = fields[1];
        String[] words = text.split(" ");
        wordfreq.clear();
        for (String w : words) {
            wordfreq.inc(w, 1);
        }
        Iterator<Entry<String, Long>> iter = wordfreq.iterator();
        long numWords = 0;
        while (iter.hasNext()) {
            Entry<String, Long> entry = iter.next();
            outkey.set(entry.getKey());
            outvalue.set("d1");
            output.collect(outkey, outvalue);
            outvalue.set("t" + entry.getValue());
            output.collect(outkey, outvalue);
            numWords += entry.getValue();
        }
        outkey.set(NUM_DOCS_STRING);
        outvalue.set("d1");
        output.collect(outkey, outvalue);
        outvalue.set("t" + numWords);
        output.collect(outkey, outvalue);
    }

    public void close() {
    }
}


