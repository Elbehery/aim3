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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BookAndAuthorBroadcastJoin extends HadoopJob {

    @Override
    public int run(String[] args) throws Exception {

        Map<String, String> parsedArgs = parseArgs(args);

        Path authors = new Path(parsedArgs.get("--authors"));
        Path books = new Path(parsedArgs.get("--books"));
        Path outputPath = new Path(parsedArgs.get("--output"));

        //IMPLEMENT ME

        Job broadCastJoin = prepareJob(books, outputPath, TextInputFormat.class, BroadCastMapper.class, Text.class, Text.class, TextOutputFormat.class);
        DistributedCache.addArchiveToClassPath(authors, broadCastJoin.getConfiguration(), FileSystem.getLocal(broadCastJoin.getConfiguration()));
        broadCastJoin.waitForCompletion(true);

        return 0;
    }

    static class BroadCastMapper extends Mapper<Object, Text, Text, Text> {

        private Map<String, String> loadedAuthorsMap = new HashMap<String, String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            // Load the Authors inside  map ONCE ONLY
            BufferedReader bufferedReader = null;

            Path[] loadedAuthors = DistributedCache.getArchiveClassPaths(context.getConfiguration());

            if (loadedAuthors != null || loadedAuthors.length != 0) {
                try {
                    bufferedReader = new BufferedReader(new FileReader(loadedAuthors[0].toString()));
                    String line;
                    while ((line = bufferedReader.readLine()) != null) {

                        String[] splitAuthorsLine = line.split("\t");
                        loadedAuthorsMap.put(splitAuthorsLine[0], splitAuthorsLine[1]);
                    }
                } finally {
                    bufferedReader.close();
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //((FileSplit) context.getInputSplit()).getPath()

            // perform the join on between authors and books on authors_ID
            String[] splitBooksLine = value.toString().split("\t");
            Set<String> authorIds = loadedAuthorsMap.keySet();
            for (String authorId : authorIds) {

                if (splitBooksLine[0].equals(authorId)) {
                    context.write(new Text(loadedAuthorsMap.get(authorId).toString()), new Text(splitBooksLine[2] + "\t" + splitBooksLine[1]));
                }

            }


        }

    }

}


