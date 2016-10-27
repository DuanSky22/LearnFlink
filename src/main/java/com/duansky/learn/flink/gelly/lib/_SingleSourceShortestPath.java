package com.duansky.learn.flink.gelly.lib;

import com.duansky.learn.flink.gelly.data.SingleSourceShortestPathsData;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.SingleSourceShortestPaths;
import org.apache.flink.types.NullValue;

/**
 * Created by DuanSky on 2016/10/27.
 */
public class _SingleSourceShortestPath {

    public static void main(String args[]){
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        testSSSP(env);
    }

    public static void testSSSP(ExecutionEnvironment env){
        System.out.println("===test single source shortest path library===");
        Graph<Long,NullValue,Double> graph =
                Graph.fromDataSet(SingleSourceShortestPathsData.getDefaultEdgeDataSet(env),env);
        try {
            DataSet<Vertex<Long, Double>> res = (DataSet<Vertex<Long, Double>>) graph.run(new SingleSourceShortestPaths(SingleSourceShortestPathsData.SRC_VERTEX_ID,10));
            res.print();
            System.out.println("----------");
            System.out.println(SingleSourceShortestPathsData.RESULTED_SINGLE_SOURCE_SHORTEST_PATHS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
