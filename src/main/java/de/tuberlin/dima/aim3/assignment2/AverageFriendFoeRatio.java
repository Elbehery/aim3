package de.tuberlin.dima.aim3.assignment2;

import com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by mustafa on 18/11/14.
 */
public class AverageFriendFoeRatio {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> input = env.readTextFile(Config.pathToSlashdotZoo());

         /* Convert the input to edges, consisting of (source, target, isFriend ) */
        DataSet<Tuple3<Long, Long, Boolean>> edges = input.flatMap(new EdgeReader());


        /* Compute the degree of every vertex with respect to friendship relation */
        DataSet<Tuple3<Long, Long, String>> verticesWithDegree =
                edges.project(0, 2).types(Long.class, Boolean.class)
                        .groupBy(0, 1).reduceGroup(new DegreeOfVertex());

        verticesWithDegree.writeAsText(Config.outputPath() + "/VertWithDegree", FileSystem.WriteMode.OVERWRITE);


        // creating dataset with only vertices that have both relationships
        DataSet<Tuple3<Long, Long, String>> verticesWithBothRelations = verticesWithDegree.groupBy(0).reduceGroup(new RelationChecker());
        verticesWithBothRelations.writeAsText(Config.outputPath() + "/VertWithbothRelation", FileSystem.WriteMode.OVERWRITE);


        // creating dataset with each vertex ratio
        DataSet<Tuple3<Long, String, Double>> friendToFoeRatioPerVertex = verticesWithBothRelations.groupBy(0).sortGroup(2, Order.DESCENDING).reduceGroup(new FriendToFoeRatioCalculator());
        friendToFoeRatioPerVertex.writeAsText(Config.outputPath() + "/friendToFoeRatioPerVertex", FileSystem.WriteMode.OVERWRITE);


        //Counting vertices with both relationships
        DataSet<Long> friendAndFoeVerticesNumber = friendToFoeRatioPerVertex.project(0).types(Long.class).distinct().reduceGroup(new VerticesWithBothRelationCounter());
        friendAndFoeVerticesNumber.writeAsText(Config.outputPath() + "/friendAndFoeVerticesNumber", FileSystem.WriteMode.OVERWRITE);

        //Computing the average ratio
        DataSet<Double> friendToFoeAverageRatio = friendToFoeRatioPerVertex.project(2).types(Double.class).reduceGroup(new FriendToFoeAverageRatioCalculator()).withBroadcastSet(friendAndFoeVerticesNumber, "friendAndFoeVerticesNumber");
        friendToFoeAverageRatio.writeAsText(Config.outputPath() + "/averageRatio", FileSystem.WriteMode.OVERWRITE);

        env.execute();

    }

    public static class EdgeReader implements FlatMapFunction<String, Tuple3<Long, Long, Boolean>> {

        private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

        @Override
        public void flatMap(String s, Collector<Tuple3<Long, Long, Boolean>> collector) throws Exception {
            if (!s.startsWith("%")) {
                String[] tokens = SEPARATOR.split(s);

                long source = Long.parseLong(tokens[0]);
                long target = Long.parseLong(tokens[1]);
                boolean isFriend = "+1".equals(tokens[2]);

                collector.collect(new Tuple3<Long, Long, Boolean>(source, target, isFriend));
            }
        }
    }

    public static class DegreeOfVertex implements GroupReduceFunction<Tuple2<Long, Boolean>, Tuple3<Long, Long, String>> {
        @Override
        public void reduce(Iterable<Tuple2<Long, Boolean>> tuples, Collector<Tuple3<Long, Long, String>> collector) throws Exception {

            Iterator<Tuple2<Long, Boolean>> iterator = tuples.iterator();

            Tuple2<Long, Boolean> temp = iterator.next();
            Long vertexId = temp.f0;
            String friend;
            if (temp.f1) {
                friend = "Friend";
            } else {
                friend = "Foe";
            }

            long count = 1L;
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }

            collector.collect(new Tuple3<Long, Long, String>(vertexId, count, friend));
        }
    }

    public static class RelationChecker implements GroupReduceFunction<Tuple3<Long, Long, String>, Tuple3<Long, Long, String>> {
        @Override
        public void reduce(Iterable<Tuple3<Long, Long, String>> input, Collector<Tuple3<Long, Long, String>> output) throws Exception {

            Iterator<Tuple3<Long, Long, String>> iterator = input.iterator();

            Set<String> relations = new HashSet<String>();
            List<Tuple3<Long, Long, String>> relationTuples = new ArrayList<Tuple3<Long, Long, String>>();
            Tuple3<Long, Long, String> tmp = null;

            while (iterator.hasNext()) {
                tmp = iterator.next();
                Tuple3<Long, Long, String> tuple3 = new Tuple3<Long, Long, String>(tmp.f0, tmp.f1, tmp.f2);
                relations.add(tuple3.f2);
                relationTuples.add(tuple3);
            }

            if (relations.size() == 2) {
                for (Tuple3<Long, Long, String> tuple : relationTuples) {
                    output.collect(tuple);
                }
            }

            //   TODO : REPORT THE BUG TO FLINK MAILING LIST
/*            List<Tuple3<Long,Long,String>> tuples = new ArrayList<Tuple3<Long, Long, String>>();
            Set<String> relations = new HashSet<String>();
            Iterator<Tuple3<Long,Long,String>> iterator = input.iterator();

            while(iterator.hasNext()){
                Tuple3<Long,Long,String> tmp = iterator.next();
                tuples.add(tmp);
                relations.add(tmp.f2);
            }

            System.out.println(relations);

            if(relations.size() == 2) {
                for(Tuple3<Long,Long,String> tuple : tuples) {
                    output.collect(tuple);
                }
            }*/
        }
    }

    public static class FriendToFoeRatioCalculator implements GroupReduceFunction<Tuple3<Long, Long, String>, Tuple3<Long, String, Double>> {

        @Override
        public void reduce(Iterable<Tuple3<Long, Long, String>> values, Collector<Tuple3<Long, String, Double>> out) throws Exception {

            Iterator<Tuple3<Long, Long, String>> iterator = values.iterator();
            List<Tuple3<Long, Long, String>> relationTuples = new ArrayList<Tuple3<Long, Long, String>>();
            Tuple3<Long, Long, String> tmp = null;

            while (iterator.hasNext()) {
                tmp = iterator.next();
                Tuple3<Long, Long, String> tuple3 = new Tuple3<Long, Long, String>(tmp.f0, tmp.f1, tmp.f2);
                relationTuples.add(tuple3);
            }

            double ratio = (double) relationTuples.get(0).f1 / (double) relationTuples.get(1).f1;

            out.collect(new Tuple3<Long, String, Double>(relationTuples.get(0).f0, "FriendToFoeRatio", ratio));

        }
    }

    public static class VerticesWithBothRelationCounter implements GroupReduceFunction<Tuple1<Long>, Long> {

        @Override
        public void reduce(Iterable<Tuple1<Long>> values, Collector<Long> out) throws Exception {

            out.collect(new Long(Iterables.size(values)));
        }
    }

    public static class FriendToFoeAverageRatioCalculator extends RichGroupReduceFunction<Tuple1<Double>, Double> {

        private long friendAndFoeVerticesNumber;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            friendAndFoeVerticesNumber = getRuntimeContext().<Long>getBroadcastVariable("friendAndFoeVerticesNumber").get(0);
        }

        @Override
        public void reduce(Iterable<Tuple1<Double>> values, Collector<Double> out) throws Exception {

            Iterator<Tuple1<Double>> iterator = values.iterator();
            double ratiosum = 0.0;
            double average;

            while (iterator.hasNext()) {
                ratiosum = ratiosum + iterator.next().f0;
            }

            average = ratiosum / (double) friendAndFoeVerticesNumber;

            out.collect(average);
        }
    }

}
