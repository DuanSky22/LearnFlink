package com.duansky.learn.flink.gelly.data;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by DuanSky on 2016/10/26.
 */
public class SimpleGraphData {
    private static final String verties ="1,1;2,2;3,3;4,4;5,5";
    private static final String edges = "1,2,0.1;1,3,0.5;1,4,0.4;" +
            "2,4,0.7;2,5,0.3;" +
            "3,4,0.2;" +
            "4,5,0.9";
    public static Graph<LongValue,LongValue,DoubleValue> getGraph(ExecutionEnvironment env){
        List<Vertex<LongValue,LongValue>> vertexList = new LinkedList<>();
        for (String s : verties.split(";")) {
            String[] tmp = s.split(",");
            Vertex<LongValue,LongValue> v = new Vertex<>(new LongValue(Long.parseLong(tmp[0])),new LongValue(Long.parseLong(tmp[1])));
            vertexList.add(v);
        }
        List<Edge<LongValue,DoubleValue>> edgeList = new LinkedList<>();
        for(String s : edges.split(";")){
            String[] tmp = s.split(",");
            Edge<LongValue,DoubleValue> e = new Edge<>(new LongValue(Long.parseLong(tmp[0])),new LongValue(Long.parseLong(tmp[1])),new DoubleValue(Double.parseDouble(tmp[2])));
            edgeList.add(e);
        }
        return Graph.fromCollection(vertexList,edgeList,env);
    }

}
