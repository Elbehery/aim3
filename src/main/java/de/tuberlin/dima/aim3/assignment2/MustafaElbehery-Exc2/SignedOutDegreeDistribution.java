package de.tuberlin.dima.aim3.assignment2;

import com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * Created by mustafa on 16/11/14.
 */
public class SignedOutDegreeDistribution {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> input = env.readTextFile(Config.pathToSlashdotZoo());

    /* Convert the input to edges, consisting of (source, target, isFriend ) */
        DataSet<Tuple3<Long, Long, Boolean>> edges = input.flatMap(new EdgeReader());


    /* Create a dataset of all vertex ids and count them */
        DataSet<Long> numVertices =
                edges.project(0).types(Long.class)
                        .union(edges.project(1).types(Long.class))
                        .distinct().reduceGroup(new CountVertices());


    /* Compute the degree of every vertex with respect to friendship relation */
        DataSet<Tuple3<Long, Long, String>> verticesWithDegree =
                edges.project(0,2).types(Long.class, Boolean.class)
                        .groupBy(0,1).reduceGroup(new DegreeOfVertex());


    /* Compute the degree distribution */
        DataSet<Tuple3<Long, Double, String>> degreeDistribution =
                verticesWithDegree.groupBy(1,2).reduceGroup(new DistributionElement())
                        .withBroadcastSet(numVertices, "numVertices");

       degreeDistribution.writeAsText(Config.outputPath(), FileSystem.WriteMode.OVERWRITE);


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

    public static class CountVertices implements GroupReduceFunction<Tuple1<Long>, Long> {
        @Override
        public void reduce(Iterable<Tuple1<Long>> vertices, Collector<Long> collector) throws Exception {
            collector.collect(new Long(Iterables.size(vertices)));
        }
    }


    public static class DegreeOfVertex implements GroupReduceFunction<Tuple2<Long,Boolean>, Tuple3<Long, Long, String>> {
        @Override
        public void reduce(Iterable<Tuple2<Long,Boolean>> tuples, Collector<Tuple3<Long, Long, String>> collector) throws Exception {

            Iterator<Tuple2<Long,Boolean>> iterator = tuples.iterator();

            Tuple2<Long,Boolean> temp = iterator.next();
            Long vertexId = temp.f0;
            String friend ;
            if(temp.f1){
                friend = "Friend";
            }
            else {
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

    public static class DistributionElement extends RichGroupReduceFunction<Tuple3<Long, Long, String>, Tuple3<Long, Double, String>> {

        private long numVertices;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            numVertices = getRuntimeContext().<Long>getBroadcastVariable("numVertices").get(0);
        }

        @Override
        public void reduce(Iterable<Tuple3<Long, Long, String>> verticesWithDegree, Collector<Tuple3<Long, Double, String>> collector) throws Exception {

            Iterator<Tuple3<Long, Long, String>> iterator = verticesWithDegree.iterator();
            Tuple3<Long, Long, String> temp = iterator.next();
            Long degree = temp.f1;
            // retrieve the friendship name of this group of degrees
            String friend = temp.f2;

            long count = 1L;
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }

            collector.collect(new Tuple3<Long, Double, String>(degree, (double) count / numVertices,friend));
        }
    }

}
