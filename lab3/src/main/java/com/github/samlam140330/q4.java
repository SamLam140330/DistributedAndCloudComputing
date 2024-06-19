package com.github.samlam140330;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class q4 {
    public static class VaccinationQ4Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text outputKey = new Text();
        private final IntWritable outputValue = new IntWritable(1);
        private final Set<String> processedDates = new HashSet<>();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String date = line.split(",")[0];
            String yearMonth = date.split("-")[0] + "-" + date.split("-")[1];
            boolean hasDose = false;

            if (!processedDates.contains(date)) {
                processedDates.add(date);
                for (int i = 3; i < 15; i++) {
                    if (Integer.parseInt(line.split(",")[i]) > 0) {
                        hasDose = true;
                        break;
                    }
                }

                if (hasDose) {
                    switch (yearMonth) {
                        case "2021-12":
                        case "2022-01":
                        case "2022-02":
                        case "2022-03":
                            outputKey.set("The numbers of days in " + yearMonth + " that have vaccination record is : ");
                            context.write(outputKey, outputValue);
                            break;
                        default:
                            break;
                    }
                }
            }
        }
    }

    public static class VaccinationQ4Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable value : values) {
                total += value.get();
            }
            outputValue.set(total);
            context.write(key, outputValue);
        }
    }
}
