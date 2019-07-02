package reducejoin;

import org.apache.hadoop.util.ToolRunner;

/*
 * $ javac -cp ~/hadoop-3.1.2/share/hadoop/client/*:~/hadoop-3.1.2/share/hadoop/common/* *.java
 * $ jar cf Join.jar *.class
 * $ hadoop-3.1.2/bin/hadoop jar Join.jar Main -Dmapreduce.map.memory.mb=1000 -Dmapreduce.map.java.opts.max.heap=800 -Dmapreduce.reduce.memory.mb=1000 -Dmapreduce.reduce.java.opts.max.heap=800 top1000.txt /user/tg/input/clickstream-enwiki-2019-04.tsv /user/tg/joined-not-sorted/ /user/tg/joined-sorted/
 * */

public class Main {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Join(), args);
        if (res == 0) {
            res = ToolRunner.run(new Sort(), args);
        }
        System.exit(res);
    }
}
