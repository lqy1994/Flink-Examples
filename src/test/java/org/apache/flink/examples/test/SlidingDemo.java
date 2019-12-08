package org.apache.flink.examples.test;


import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author liqingyong02
 */
public class SlidingDemo {
    public static void main(String[] args) {
        int[] ts = {1, 2, 3, 4, 7, 8, 16, 32};
        int size = 10;
        int slide = 5;
        Map<Integer, Integer> map = new HashMap<>();
        for (int timestamp : ts) {
            int lastStart = timestamp - (timestamp + slide) % slide;
            for (int start = lastStart; start > timestamp - size; start -= slide) {
                map.put(start, start + size);
            }
        }
        final Map<Integer, Integer> finalMap = new LinkedHashMap<>();
        map.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey))
                .forEachOrdered(e -> finalMap.put(e.getKey(), e.getValue()));
        System.out.println(finalMap);
    }
}
