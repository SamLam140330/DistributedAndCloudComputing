package com.github.samlam140330;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class q1 {
    public static class VaccinationQ1Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private final NullWritable outputKey = NullWritable.get();
        private final Text outputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String target = "19-Dec";
            String replacement = "12-19";
            line = line.replaceAll(target, replacement);
            outputValue.set(line);
            context.write(outputKey, outputValue);
        }
    }

    public static class VaccinationQ1Reducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }
}
