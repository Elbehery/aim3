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

package de.tuberlin.dima.aim3.assignment4;


import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.Map;

public class Classification {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSource<String> conditionalInput = env.readTextFile(Config.pathToConditionals());
    DataSource<String> sumInput = env.readTextFile(Config.pathToSums());

    DataSet<Tuple3<String, String, Long>> conditionals = conditionalInput.map(new ConditionalReader());
    DataSet<Tuple2<String, Long>> sums = sumInput.map(new SumReader());

    DataSource<String> testData = env.readTextFile(Config.pathToTestSet());

    DataSet<Tuple3<String, String, Double>> classifiedDataPoints = testData.map(new Classifier())
        .withBroadcastSet(conditionals, "conditionals")
        .withBroadcastSet(sums, "sums");

    classifiedDataPoints.writeAsCsv(Config.pathToOutput(), "\n", "\t", FileSystem.WriteMode.OVERWRITE);

    env.execute();
  }

  public static class ConditionalReader implements MapFunction<String, Tuple3<String, String, Long>> {

    @Override
    public Tuple3<String, String, Long> map(String s) throws Exception {
      String[] elements = s.split("\t");
      return new Tuple3<String, String, Long>(elements[0], elements[1], Long.parseLong(elements[2]));
    }
  }

  public static class SumReader implements MapFunction<String, Tuple2<String, Long>> {

    @Override
    public Tuple2<String, Long> map(String s) throws Exception {
      String[] elements = s.split("\t");
      return new Tuple2<String, Long>(elements[0], Long.parseLong(elements[1]));
    }
  }


  public static class Classifier extends RichMapFunction<String, Tuple3<String, String, Double>>  {

     private final Map<String, Map<String, Long>> wordCounts = Maps.newHashMap();
     private final Map<String, Long> wordSums = Maps.newHashMap();

     @Override
     public void open(Configuration parameters) throws Exception {
       super.open(parameters);

       Iterable<Tuple3<String, String, Long>> myConditionals = getRuntimeContext().getBroadcastVariable("conditionals");
       Iterable<Tuple2<String, Long>> mySums = getRuntimeContext().getBroadcastVariable("sums");

       for (Tuple2<String, Long> value : mySums) {
         wordSums.put(value.f0, value.f1);
       }

       for (String label: wordSums.keySet()) {
         wordCounts.put(label, Maps.<String, Long>newHashMap());
       }

       for (Tuple3<String, String, Long> value : myConditionals) {
         wordCounts.get(value.f0).put(value.f1, value.f2);
       }
     }

     private double getWordProbability(String term, String documentLabel){
       long count = 0;
       if (wordCounts.get(documentLabel).containsKey(term)) {
         count = wordCounts.get(documentLabel).get(term);
       }

       return (double) (count + Config.getSmoothingParameter()) / (double) (wordSums.get(documentLabel) +
             Config.getSmoothingParameter() * wordSums.keySet().size());
     }

     @Override
     public Tuple3<String, String, Double> map(String line) throws Exception {

       String[] tokens = line.split("\t");
       String label = tokens[0];
       String[] terms = tokens[1].split(",");

       double maxProbability = Double.NEGATIVE_INFINITY;
       String argMaxLabel = "";

       for (String currentLabel : wordCounts.keySet()) {

         double currentProbability = 0.0;
         for (String term: terms) {
           currentProbability += Math.log(getWordProbability(term, currentLabel));
         }

         // we assume a uniform prior which can be ignored for the argmax computation
         if (maxProbability <= currentProbability){
           maxProbability = currentProbability;
           argMaxLabel = currentLabel;
         }
       }

       return new Tuple3<String, String, Double>(label, argMaxLabel, maxProbability);
     }
   }

}
