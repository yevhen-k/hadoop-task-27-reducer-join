# Hadoop Reduce Join

## Task

Use Hadoop Broadcast Join to join two datasets available [here](https://dumps.wikimedia.org/other/clickstream/)

For the first month get 1000 most popular pages.

For the second one make join with 1000 most popular pages of the first month.

## Solution

### Preparation

`$ wget https://dumps.wikimedia.org/other/clickstream/2019-03/clickstream-enwiki-2019-03.tsv.gz`

`$ wget https://dumps.wikimedia.org/other/clickstream/2019-04/clickstream-enwiki-2019-04.tsv.gz`

`$ gzip -d *.gz`

`$ hadoop fs -mkdir -p /user/tg/input`

`$ hadoop fs -put *.tsv /user/tg/input`

`$ export JAVA_HOME=/usr/java/default`

**USE JAVA 8 ONLY!**

`export PATH=${JAVA_HOME}/bin:${PATH}`

`export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar`

### Step 1

Compile .jar for mapreduce data for firs month:

`$ cd reducesort`

`$ javac -cp ~/hadoop-3.1.2/share/hadoop/client/*:~/hadoop-3.1.2/share/hadoop/common/* *.java`

`$ jar cf GetTop1000AndSort.jar *.class`

`$ rm *.class`

Run first job

`$ cd`

`$ hadoop-3.1.2/bin/hadoop jar GetTop1000AndSort.jar Main -Dmapreduce.map.memory.mb=1000 -Dmapreduce.map.java.opts.max.heap=800 -Dmapreduce.reduce.memory.mb=1000 -Dmapreduce.reduce.java.opts.max.heap=800 /user/tg/input/clickstream-enwiki-2019-03.tsv /user/tg/mapred/ /user/tg/sorted/`

`$ hadoop-3.1.2/bin/hadoop fs -text /user/tg/sorted/* | head -n1000 > top1000.txt`

### Step 2

Compile .jar for reduce side join

`$ cd reducejoin`

`$ javac -cp ~/hadoop-3.1.2/share/hadoop/client/*:~/hadoop-3.1.2/share/hadoop/common/* *.java`

`$ jar cf Join.jar *.class`

`$ rm *.class`

`$ cd`

Run reduce side join job

`$ hadoop-3.1.2/bin/hadoop fs -put top1000.txt /user/tg/`

`$ hadoop-3.1.2/bin/hadoop jar Join.jar Main -Dmapreduce.map.memory.mb=1000 -Dmapreduce.map.java.opts.max.heap=800 -Dmapreduce.reduce.memory.mb=1000 -Dmapreduce.reduce.java.opts.max.heap=800 top1000.txt /user/tg/input/clickstream-enwiki-2019-04.tsv /user/tg/joined-not-sorted/ /user/tg/joined-sorted/`

`$ hadoop-3.1.2/bin/hadoop fs -text /user/tg/joined-sorted/* > topJoined.txt`