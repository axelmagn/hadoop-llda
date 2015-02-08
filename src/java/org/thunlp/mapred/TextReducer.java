package org.thunlp.mapred;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public abstract class TextReducer implements Reducer<Text, Text, Text, Text> {

    public abstract void reduce(Text key, Iterator<Text> values,
                                OutputCollector<Text, Text> collector,
                                Reporter reporter) throws IOException;

    public void configure(JobConf conf) {
    }

    public void close() throws IOException {
    }

}