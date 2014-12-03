/**
 * AIM3 - Scalable Data Mining -  course work
 * Copyright (C) 2014  Sebastian Schelter
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.tuberlin.dima.aim3.assignment1;

import de.tuberlin.dima.aim3.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;

public class AverageTemperaturePerMonth extends HadoopJob {

    @Override
    public int run(String[] args) throws Exception {
        Map<String, String> parsedArgs = parseArgs(args);

        Path inputPath = new Path(parsedArgs.get("--input"));
        Path outputPath = new Path(parsedArgs.get("--output"));

        double minimumQuality = Double.parseDouble(parsedArgs.get("--minimumQuality"));

        //IMPLEMENT ME

        Job averageTemperature = prepareJob(inputPath, outputPath, TextInputFormat.class, AverageTemperatureMapper.class,
                Text.class, DoubleWritable.class, AverageTemperatureReducer.class, Text.class, DoubleWritable.class, TextOutputFormat.class);
        averageTemperature.getConfiguration().set("minimumQuality", Double.toString(minimumQuality));
        averageTemperature.waitForCompletion(true);

        return 0;
    }

    static class AverageTemperatureMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\t");
            double recordQuality = Double.parseDouble(split[3]);

            if (!(recordQuality < Double.parseDouble(context.getConfiguration().get("minimumQuality")))) {

                String yearAndMonth = split[0] + "\t" + split[1];
                DoubleWritable temperature = new DoubleWritable(Double.parseDouble(split[2]));
                Text preReduceKey = new Text(yearAndMonth);
                context.write(preReduceKey, temperature);

            }

        }
    }

    static class AverageTemperatureReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            DoubleWritable result = new DoubleWritable();
            double sum = 0.0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum = sum + val.get();
                count++;
            }

            double average = sum / count;
            result.set(average);

            context.write(key, result);
        }
    }
}