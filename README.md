### build

```bash
#提交时转换为LF，检出时不转换
git config --global core.autocrlf input    
#拒绝提交包含混合换行符的文件
git config --global core.safecrlf true
```
sed -i s/spark-parent_2.11/spark-parent_2.12/g `grep spark-parent_2.11 -rl --include="pom.xml" ./`
mvn clean package -DskipTests -Pmesos -Pyarn -Phive 

./dev/make-distribution.sh --tgz --mvn $MAVEN_HOME/bin/mvn -Phive -Pyarn -Pmesos -DskipTests

### standalone mode
 vi conf/*
 start-all.sh 

 ###
 [spark-rpc](https://github.com/spafka/spark/tree/master/MDZ/spark-rpc.md)
 ###
 [Spark on Mesos](https://github.com/spafka/spark/tree/master/MDZ/spark-on-mesos.md)
 ###
 [RDD](https://github.com/spafka/spark/tree/master/MDZ/RDD.md)
 ###
 [spark-submit](https://github.com/spafka/spark/tree/master/MDZ/spark-submit.md)
 ###
 [CoarseGrainedExecutorBackend](https://github.com/spafka/spark/tree/master/MDZ/CoarseGrainedExecutorBackend.md)
 ###
 [spark-dagScheduler.md](https://github.com/spafka/spark/tree/master/MDZ/spark-dagScheduler.md)
 ###
 [spark-taskcheduler.md](https://github.com/spafka/spark/tree/master/MDZ/spark-taskcheduler.md)
 ###
 [3md](https://github.com/spafka/spark/tree/master/MDZ/3.md)
 ###
 [ZhenHeSparkRDDAPIExamples](http://homepage.cs.latrobe.edu.au/zhe/ZhenHeSparkRDDAPIExamples.html)
 ### 
 [SPARK STREAMING](https://github.com/lw-lin/CoolplaySpark)
 ###
 [Spark_SQL fork from summerDG](https://github.com/spafka/spark/tree/master/MDZ/3rd/README.md)
 ###
 [SparkPlan example](https://github.com/spafka/spark/tree/master/MDZ/sparkPlan-example.md)
  