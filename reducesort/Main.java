package reducesort;

import org.apache.hadoop.util.ToolRunner;

/*
* $ javac -cp ~/hadoop-3.1.2/share/hadoop/client/*:~/hadoop-3.1.2/share/hadoop/common/* *.java
* $ jar cf Sort.jar *.class
* $ hadoop-3.1.2/bin/hadoop jar GetTop1000AndSort.jar Main -Dmapreduce.map.memory.mb=1000 -Dmapreduce.map.java.opts.max.heap=800 -Dmapreduce.reduce.memory.mb=1000 -Dmapreduce.reduce.java.opts.max.heap=800 /user/tg/input/clickstream-enwiki-2019-03.tsv /user/tg/mapred/ /user/tg/sorted/
* */

public class Main {
    public static void main(String[] args) throws Exception {
        int res1 = ToolRunner.run(new MapRedTop1000(), args);
        int res2 = 0;
        if (res1 == 0) {
            res2 = ToolRunner.run(new Sort(), args);
        } else {
            System.exit(res1);
        }
        System.exit(res2);
    }
}
