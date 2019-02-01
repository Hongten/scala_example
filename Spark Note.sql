# scala_example

# Spark
1.Saprk是分布式计算框架
2.Spark于MR的区别
2.1.Spark基于内存迭代处理数据，MR基于磁盘迭代处理数据
2.2.Spark中有DAG有向无环图，执行引擎，执行速度快
3.Spark底层操作的是RDD

# Spark技术栈
HDFS, MR, Yarn, Hive, Storm, SparkCore, SparkStreaming, SparkSQL, SparkMIlib

# Spark运行模式
1.local - 用于本地eclipse开发，多用于测试
2.standalone - Spark自带的资源调度框架，支持分布式搭建，Spark可以基于standalone运行任务
3.yarn - Hadoop生态圈中的资源调度框架，Spark也支持Yarn中运行
4.mesos - 资源调度框架

# Spark代码流程
1.创建val conf = new SparkConf().setMaster("local").setAppName("test")
2.创建SparkContext, val sc = new SparkContext(conf)
3.获取文件内容val lines = sc.textFile("./filename")
4.对获取到的RDD使用Transformation算子进行数据转换
5.要使用Action算子对Transformation算子触发执行
6.sc.stop()

# Spark核心RDD
RDD(Resilient Distributed Dataset) - 弹性分布式数据集
RDD五大特性：
1.RDD由一系列的partition组成
2.算子是作用在partition上的
3.RDD之间有依赖关系
4.分区器是作用在K，V格式的RDD上
5.partition对外提供最佳的计算位置，利于数据处理的本地化


# 问题
1.什么是K，V格式的RDD
RDD中高端元素是一个二元组Tuple2<K, V>()

2.sc.textFile("./filename")底层是调用MR读取HDFS的方法，首先也会split，一个split对应一个block，这里的split也对应一个partition

3.哪里体现RDD的分布式
RDD中的partition是分布在多个节点上的

4.哪里体现RDD的弹性
1.partition的个数可多可少
2.RDD之间有依赖关系

# RDD中是不存数据的
RDD中是不存数据的，partition也不数据的。

# 算子
1.Transformation算子（懒执行，需要Action算子触发执行）
filter
map
mapToPair
flatMap
reduceByKey
sortBy
sortByKey
sample
join
leftOuterJoin
rightOuterJoin
fullOuterJoin
union
intersection
subtract
distinct
cogroup
mapPartitions

2.Action算子（触发Transformation算子执行，一个application中有几个Action算子，就有几个job）
foreach
count
first
take(num)
collect
foreachPartition

3.持久化算子
cache 
--默认将数据存在内存中 cache() = persist() = persist(StorageLevel.MEMORY_ONLY)

persist
--可以手动指定数据持久化级别

private var _useDisk: Boolean,
private var _useMemory: Boolean,
private var _useOffHeap: Boolean,
private var _deserialized: Boolean,
private var _replication: Int = 1

object StorageLevel {
val NONE = new StorageLevel(false, false, false, false)
val DISK_ONLY = new StorageLevel(true, false, false, false)
val DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2)
val MEMORY_ONLY = new StorageLevel(false, true, false, true)
val MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2)
val MEMORY_ONLY_SER = new StorageLevel(false, true, false, false)
val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)
val MEMORY_AND_DISK = new StorageLevel(true, true, false, true)
val MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2)
val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false)
val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2)
val OFF_HEAP = new StorageLevel(true, true, true, false, 1)

...
}

注意：
a.尽量避免使用DISK_ONLY级别
b.尽量避免使用_2级别


checkpoint
--1.将数据持久化到磁盘
--2.可以切断RDD之间的依赖关系

场景：当lineage非常长，计算还有复杂时，可以使用checkpoint对RDD进行持久化，当application执行完，checkpoint中的数据不会被清除



# Spark 集群搭建
--准备四个节点node1，node2，node3，node4

--操作node1
--上传spark-1.6.0-bin-hadoop2.6.tgz到node1上
--解压spark-1.6.0-bin-hadoop2.6.tgz
tar -zxvf spark-1.6.0-bin-hadoop2.6.tgz

--解压后，重新命名
mv  spark-1.6.0-bin-hadoop2.6/ spark-1.6.0

--配置
cd /root/spark-1.6.0/conf/

--使用salves.template复制一份，命名为slaves
cp salves.template slaves


--编辑slaves文件
vi slaves


--配置spark worker节点
node2
node3
node4

:wq


--使用spark-env.sh.template复制一份，命名为spark-env.sh
cp spark-env.sh.template spark-env.sh

--编辑spark-env.sh文件
vi spark-env.sh

--设置spark master节点，提交任务端口号，worker节点核数和内存大小
export SPARK_MASTER_IP=node1
export SPARK_MASTER_PORT=7077
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=2g

--可以修改webui端口号，默认端口号为8080
export SPARK_MASTER_WEBUI_PORT=8888


:wq


--配置完成
--将node1上面配置好的spark文件，分发到node2，node3，node4上面去
cd ~
scp -r /root/spark-1.6.0/ root@node2:/root/
scp -r /root/spark-1.6.0/ root@node3:/root/ 
scp -r /root/spark-1.6.0/ root@node4:/root/



--在主节点node1上面启动spark
cd /root/spark-1.6.0/sbin/
./start-all.sh

--webUI，默认端口号为8080
http://node1:8888


--在主节点node1上面停止spark
cd /root/spark-1.6.0/sbin/
./stop-all.sh




--运行exampe里面的SparkPi
/root/spark-1.6.0/bin

--standalone模式提交任务 - client方式提交任务 - 默认模式
./spark-submit --master spark://node1:7077 --class org.apache.spark.examples.SparkPi ../lib/spark-examples-1.6.0-hadoop2.6.0.jar 100

./spark-submit --master spark://node1:7077 --deploy-mode client --class org.apache.spark.examples.SparkPi ../lib/spark-examples-1.6.0-hadoop2.6.0.jar 100


standalone模式提交任务 - client方式提交任务流程 - 默认模式
1.Worker启动，并且向Master汇报信息
2.在客户端提交任务，Driver会在客户端启动
3.Driver向Master申请资源
4.Master找到一批符合的Worker，在Worker中启动Executor。并把Worker信息返回给Driver
5.Driver发送Task给Worker
6.Worker接收Task，执行任务，返回计算结果给Driver
7.Driver接收Worker计算的结果


--总结：
在这种模式下面，我们在客户端看到Task执行的详细信息还有最终的结果。
当在客户端提交多个application时，每个application都会启动自己的Driver
这些Driver和Worker有大量的通行，会造成客户端网卡浏览暴增问题，
这种模式适合于测试，不适合生产环境。


--standalone模式提交任务 - cluster方式提交任务
./spark-submit --master spark://node1:7077 --deploy-mode cluster --class org.apache.spark.examples.SparkPi ../lib/spark-examples-1.6.0-hadoop2.6.0.jar 100


standalone模式提交任务 - cluster方式提交任务流程
1.Worker启动，并且向Master汇报信息
2.在客户端提交任务，客户端向Master申请启动Driver
3.Master收到客户端请求后，随机在一台Worker节点上启动Driver
4.启动好的Driver向Master申请资源
5.Master接收到Driver的申请后，找到一批符合的Worker，在Worker中启动Executor。并把Worker信息返回给Driver
6.Driver发送Task给Worker
7.Worker接收Task，执行任务，返回计算结果给Driver
8.Driver接收Worker计算的结果（在WebUI上Completed Drivers里面可以查看到结果和执行的详细信息）


--总结
在这种模式下面，我们在客户端看不到Task执行的详细信息和结果。
可以在WebUI上Completed Drivers里面可以查看到结果和执行的详细信息
如果在客户端提交多个application，那么每个application的Driver会被分散到集群的Worker节点中，
相当于将客户端模式的客户端网卡流量暴增问题分散到集群中。这种模式适合生产环境。




--yarn模式提交任务 --Client方式 --默认方式
./spark-submit --master yarn --class org.apache.spark.examples.SparkPi ../lib/spark-examples-1.6.0-hadoop2.6.0.jar 100

./spark-submit --master yarn --deploy-mode client --class org.apache.spark.examples.SparkPi ../lib/spark-examples-1.6.0-hadoop2.6.0.jar 100
./spark-submit --master yarn-client --class org.apache.spark.examples.SparkPi ../lib/spark-examples-1.6.0-hadoop2.6.0.jar 100


--yarn模式提交任务 --Client方式 --默认方流程
1.NodeManager启动，并且向ResourceManager汇报信息，ResourceManager掌握集群资源
2.在客户端提交任务，Driver会在客户端启动
3.Driver向ResourceManager申请资源
4.ResourceManager随机找一台NodeManager启动ApplicationMaster
5.启动好的ApplicationMaster向ResourceManager申请资源用户启动Executor，ResourceManager返回一批NodeManager节点给ApplicationMaster
6.ApplicationMaster去连接ResourceManager返回来的NodeManager去启动Executor，Executor里面有线程池(Thread Pool)
7.Executor启动完成之后，会反向注册给客户端的Driver
8.Driver向Executor发送Task
9.Executor接收到Task，执行任务，并把计算结果返回给Driver
10.Driver接收Executor计算的结果

总结：
在这种模式下面，我们在客户端看到Task执行的详细信息还有最终的结果。
当在客户端提交多个application时，每个application都会启动自己的Driver
这些Driver和Executor(NodeManager)有大量的通行，会造成客户端网卡浏览暴增问题，
这种模式适合于测试，不适合生产环境。




--yarn模式提交任务 --Custer方式
./spark-submit --master yarn --deploy-mode cluster --class org.apache.spark.examples.SparkPi ../lib/spark-examples-1.6.0-hadoop2.6.0.jar 100

./spark-submit --master yarn-cluster --class org.apache.spark.examples.SparkPi ../lib/spark-examples-1.6.0-hadoop2.6.0.jar 100

--yarn模式提交任务 --Custer方式流程
1.NodeManager启动，并且向ResourceManager汇报信息，ResourceManager掌握集群资源
2.在客户端提交任务，客户端向ResourceManager申请启动ApplicationMaster
3.ResourceManager随机找一台NodeManager启动ApplicationMaster，此时的ApplicationMaster就是Driver
4.ApplicationMaster启动之后，向ResourceManager申请资源启动Executor
5.ResourceManager向ApplicationMaster返回一批Executor资源
6.ApplicationMaster连接并启动Executor
7.Executor启动之后，反向注册给ApplicationMaster（Driver）
8.ApplicationMaster发送Task给Executor
9.Executor接收Task，执行任务，并把结果返回给ApplicationMaster
10.ApplicationMaster接收Executor计算的结果（结果可以在Yarn的WebUI上面查看）

总结：
在这种模式下面，我们在客户端看不到Task执行的详细信息还有最终的结果。
可以在WebUI上Completed Drivers里面可以查看到结果和执行的详细信息
如果在客户端提交多个application，那么每个application的ApplicationMaster会被分散到集群的NodeManager节点中，
相当于将客户端模式的客户端网卡流量暴增问题分散到集群中。这种模式适合生产环境。


--Driver的功能：
1.发送Task
2.监控Task
3.申请资源
4.回收结果




--术语
Master：standalone模式下面，资源管理的主节点，主进程
Cluster Manager:在集群上获取资源的外部服务
Worker：standalone模式下面，资源管理的从节点，从进程，管理本机资源的进程
Application：基于Spark的用户程序，包含了Driver程序和运行在集群上的Executor程序
Drive：用来连接Worker的程序
Executor：是一个Worker进程所管理的节点上为某Application启动的一个进程，
                    该进程负责运行任务，并且负责将数据存在内存或磁盘。
                    每个应用都有各自独立的Executors。
                    工作实际工作进程。
Task：被Driver发送到Executor上被执行的工作单元
Job：包含很多任务的Task的并行计算，可以看做和Action算子对应
Stage：一个Job会被拆分为很多Task，每组Task被称为Stage（类似MapReduce中的Map Task和Reduce Task）
            由一组并行的Task组成。


--任务
Application - 由Job组成（看Action算子） - 由Stage组成 - 由一组Task组成

--资源
主节点Master - 从节点Worker - 启动Executor - Thread Pool

Task就是被发送到Executor里面的Thread Pool上执行的。




--RDD宽窄依赖
RDD之间有依赖关系
窄依赖：
父RDD和子RDD partition之间的依赖关系是一对一， 如：map, filter
父RDD和子RDD partition之间的依赖关系是多对一，如union
宽依赖：
父RDD和子RDD partition之间的依赖关系是一对多，如：groupBykey
在款依赖中有Shuffle，也即有I/O操作（网络，磁盘）



--Spark处理数据模式， 即管道pipeline计算模式
f3(f2(f1()))高阶函数展开形式处理数据
在一个Stage中，基于内存，一条一条的处理数据

MapReduce：
1+1=2 -> 结果2落地 -> 2+1=3

Spark：
1+1+1=3 -> 在同一Stage里面结果没有落地


--Stage的并行度
由Stage中最后的RDD的partition个数决定

--如何提高Stage的并行度
在有Shuffle的算子里面设置numPartition
如：reduceByKey(rdd, numPartition), join(rdd, numPartition)


--管道中的数据，上面时候落地
1.shuffle write
2.对RDD进程持久化操作（cache，persist， checkpoint）










