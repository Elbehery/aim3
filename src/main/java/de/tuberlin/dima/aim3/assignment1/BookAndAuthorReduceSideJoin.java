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
import org.apache.hadoop.filecache.DistributedCache;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class BookAndAuthorReduceSideJoin extends HadoopJob {


    @Override
    public int run(String[] args) throws Exception {

        Map<String, String> parsedArgs = parseArgs(args);

        Path authors = new Path(parsedArgs.get("--authors"));
        Path books = new Path(parsedArgs.get("--books"));
        Path outputPath = new Path(parsedArgs.get("--output"));

        // IMPLEMENT ME

        Path reduceSideInputPath = new Path(authors + "," + books);

        Job reduceSideJoin = prepareJob(reduceSideInputPath, outputPath, TextInputFormat.class, ReduceSideMapper.class, Text.class, Text.class,
                ReduceSideReducer.class, Text.class, Text.class, TextOutputFormat.class);
        reduceSideJoin.waitForCompletion(true);

        return 0;
    }


    static class ReduceSideMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String authorType = "author";
            String bookType = "book";
            String[] splitBooksLine = value.toString().split("\t");
            if (splitBooksLine.length == 3) {
                context.write(new Text(splitBooksLine[0]), new Text(splitBooksLine[1] + "\t" + splitBooksLine[2] + "\t" + bookType));
            } else if (splitBooksLine.length == 2) {
                context.write(new Text(splitBooksLine[0]), new Text(splitBooksLine[1] + "\t" + authorType));
            }
        }
    }


    static class ReduceSideReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> books = new ArrayList<String>();
            String author = null;

            for (Text val : values){

                String[ ] splitValues = val.toString().split("\t");
                if(splitValues[1].equals("author") ){
                    author = splitValues[0];
                }
                else{
                    String temp = splitValues[1]+"\t"+splitValues[0];
                    books.add(temp);
                }
            }

            for (String book : books){
                context.write(new Text(author), new Text(book));
            }



        }
    }

}

