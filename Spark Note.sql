# scala_example

# Spark
1.Saprk是分布式计算框架
2.Spark于MR的区别
2.1.Spark基于内存迭代处理数据，MR基于磁盘迭代处理数据
2.2.Spark中有DAG有向无环图，执行引擎，执行速度快
2.3.Spark粗粒度资源申请，MP细粒度资源申请
2.4.MR中有mapper，reducer，相当于Spark中的map和reduceByKey两个算子。在MR业务逻辑中要自己实现其他算子，而Spark已经提供了各种对应业务的算子。
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


--管道中的数据，什么时候落地
1.shuffle write
2.对RDD进程持久化操作（cache，persist， checkpoint）





--Spark资源调度和任务调度
一个Application，在这个application里面是由job组成（有多少Action算子），
job是由RDD组成。RDD之间形成DAG有向无环图。这个DAG有向五环图会提交给
一个叫DAGScheduler对象--任务调度的高度调度器，DAGScheduler对象负责把
DAG按照RDD的宽窄依赖进行切分成一个一个的Stage，并且把这些Stage封装到TaskSet
对象里面。TaskSet由提交给TaskScheduler对象--任务调度的底层调度器，TaskScheduler
会遍历TaskSet，拿出一个一个的Task发送到Worker节点上Executor的Thread Pool去执行


当TaskScheduler发送到Worker的Task失败时，需要重试3次发送，如果3次以后依然失败，
TaskScheduler就不管了。由这个Task对应的TaskSet转换来的Stage-由DAGScheduler重试
这个Stage，把Stage封装为TaskSet到TaskScheduler，这一个过程中如果失败，DAGScheduler
会重试4次，如果4次都失败，那么这个Stage所在的Job就失败，所以这个Application执行失败。



--https://blog.csdn.net/LHWorldBlog/article/details/79300025
Spark资源调度和任务调度的流程：
1、启动集群后，Worker节点会向Master节点汇报资源情况，Master掌握了集群资源情况。
2、当Spark提交一个Application后，根据RDD之间的依赖关系将Application形成一个DAG有向无环图。任务提交后，Spark会在Driver端创建两个对象：DAGScheduler和TaskScheduler。
3、DAGScheduler是任务调度的高层调度器，是一个对象。DAGScheduler的主要作用就是将DAG根据RDD之间的宽窄依赖关系划分为一个个的Stage，然后将这些Stage以TaskSet的形式提交给TaskScheduler（TaskScheduler是任务调度的低层调度器，这里TaskSet其实就是一个集合，里面封装的就是一个个的task任务,也就是stage中的并行度task任务）
4、TaskSchedule会遍历TaskSet集合，拿到每个task后会将task发送到计算节点Executor中去执行（其实就是发送到Executor中的线程池ThreadPool去执行）。
5、task在Executor线程池中的运行情况会向TaskScheduler反馈，
6、当task执行失败时，则由TaskScheduler负责重试，将task重新发送给Executor去执行，默认重试3次。如果重试3次依然失败，那么这个task所在的stage就失败了。
7、stage失败了则由DAGScheduler来负责重试，重新发送TaskSet到TaskSchdeuler，Stage默认重试4次。如果重试4次以后依然失败，那么这个job就失败了。job失败了，Application就失败了。
8、TaskScheduler不仅能重试失败的task,还会重试straggling（落后，缓慢）task（也就是执行速度比其他task慢太多的task）。如果有运行缓慢的task那么TaskScheduler会启动一个新的task来与这个运行缓慢的task执行相同的处理逻辑。两个task哪个先执行完，就以哪个task的执行结果为准。这就是Spark的推测执行机制。在Spark中推测执行默认是关闭的。推测执行可以通过spark.speculation属性来配置。

总结：
1、对于ETL类型要入数据库的业务要关闭推测执行机制，这样就不会有重复的数据入库。
2、如果遇到数据倾斜的情况，开启推测执行则有可能导致一直会有task重新启动处理相同的逻辑，任务可能一直处于处理不完的状态。（所以一般关闭推测执行）
3、一个job中多个action， 就会有多个job，一般一个action对应一个job,如果一个application中有多个job时，按照顺序一次执行，即使后面的失败了，前面的执行完了就完了，不会回滚。
4、有SparkContext端就是Driver端。
5、一般到如下几行时，资源就申请完了，后面的就是处理逻辑了
val conf = new SparkConf()
conf.setMaster("local").setAppName("Spark Resource Apply Test");
val sc = new SparkContext(conf)

粗粒度资源申请和细粒度资源申请
粗粒度资源申请(Spark）

在Application执行之前，将所有的资源申请完毕，当资源申请成功后，才会进行任务的调度，当所有的task执行完成后，才会释放这部分资源。
优点：在Application执行之前，所有的资源都申请完毕，每一个task运行时直接使用资源就可以了，不需要task运行时在执行前自己去申请资源，task启动就快了，task执行快了，stage执行就快了，job就快了，application执行就快了。
缺点：直到最后一个task执行完成才会释放资源，集群的资源无法充分利用。当数据倾斜时更严重。

细粒度资源申请（MapReduce）
Application执行之前不需要先去申请资源，而是直接执行，让job中的每一个task在执行前自己去申请资源，task执行完成就释放资源。
优点：集群的资源可以充分利用。
缺点：task自己去申请资源，task启动变慢，Application的运行就相应的变慢了。



--Spark资源调度
1.Executor在集群中分散启动，利于数据处理的本地化
2.如果提交任务不指定--executor-cores，集群默认在每个Worker上启动1个Executor，这个Executor会使用这台Worker上面的所有Core和1G内存
3.如果想要在一台Worker上面启动多个Executor，那么提交任务要指定--executor-cores
4.启动Executor不仅和Core有关，还和内存有关
5.提交Application要指定--total-executor-cores，否则，当前Application会使用集群所有的Core


--广播变量 - 当Executor端使用到Driver端的变量
1.不使用广播变量，Executor中有多少task就有多少变量副本
2.使用广播变量，每个Executor只有一份Driver端的变量

注意：
1.不可以将RDD广播出去，可以将RDD的结果广播出去
2.只能在Driver端定义，在Executor端不能改变



--累加器 - accumulator相当于集群中统筹大变量
1.累加器只能在Driver定义并初始化，不能再Executor端定义初始化
2.累加器取值accumulator.value只能在Driver读取，不能在Executor端accumulator.value读取值



--spark-shell
./spark-shell --master spark://node1:7077,node2:7077

sc.textFile("hdfs://node1:8020/usr/input/wordcount1/word3.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)

spark-job
http://node1:4040

sc.textFile("hdfs://node1:8020/usr/input/wordcount2").cache().collect();
http://node1:4040/storage/

--删除HDFS上面的文件
./hdfs dfs -rm -r /usr/input/wordcount/word2.txt._COPYING_


---------------------------------------------------
--启用spark日志
./spark-shell --master spark://node1:7077 --name aaa1 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://node1:8020/usr/input/wordcount


--配置spark日志
cd /home/spark-1.6.0/conf
cp spark-defaults.conf.template spark-defaults.conf
vi spark-defaults.conf

--启用spark日志
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs://node1:8020/spark/log

:wq


cd /home/spark-1.6.0/bin
./spark-shell --master spark://node1:7077 --name bbc



---------------------------------------------------
--启用历史日志
cd /home/spark-1.6.0/conf
cp spark-defaults.conf.template spark-defaults.conf

--配置历史日志目录，是否压缩日志：true，default为false
--压缩日志文件占用空间少
spark.history.fs.logDirectory               hdfs://node1:8020/spark/log
spark.eventLog.compress                     true


:wq


cd /home/spark-1.6.0/sbin/
./start-history-server.sh

--浏览器输入，
http://node1:18080/




---------------------------------------------------
--配置Spark Master HA - 指的是Standalone集群
--使用ZooKeeper - 分布式协调服务，保存元数据，自动切换
--在node1上面配置
cd /home/spark-1.6.0/conf/

vi spark-env.sh

export SPARK_DAEMON_JAVA_OPTS="-Dspark.deploy.recoveryMode=ZOOKEEPER -Dspark.deploy.zookeeper.url=node1:2181,node2:2181,node3:2181 -Dspark.deploy.zookeeper.dir=/opt/spark"

:wq

--分发到node2，node3，node4上面去
scp ./spark-env.sh root@node2:/home/spark-1.6.0/conf/
scp ./spark-env.sh root@node3:/home/spark-1.6.0/conf/
scp ./spark-env.sh root@node4:/home/spark-1.6.0/conf/


--进入node2
cd /home/spark-1.6.0/conf/
vi spark-env.sh

export SPARK_MASTER_IP=node2


--配置完毕
--进入node1,启动spark
cd /home/spark-1.6.0/sbin
./start-all.sh

--浏览器输入
http://node1:8080/


--进入node2，单独启动master，此时这个master为standby
cd /home/spark-1.6.0/sbin
./start-master.sh 


--浏览器输入
http://node2:8080/

--状态变化
STANDBY
RECOVERING
ALIVE


--提交任务
/root/spark-1.6.0/bin
./spark-submit --master spark://node1:7077,node2:7077 --class org.apache.spark.examples.SparkPi ../lib/spark-examples-1.6.0-hadoop2.6.0.jar 100





---------------------------------------------------
--Spark Shuffle
--参考：https://blog.csdn.net/zhanglh046/article/details/78360762

reduceByKey的含义
reduceByKey会将上一个RDD中的每一个key对应的所有value聚合成一个value，
然后生成一个新的RDD，元素类型是<key, value>对的形式，这样每一个key对
一个聚合起来的value


如何聚合
Shuffle Write：上一个stage的每个map task就必须保证将自己处理的
当前分区中的数据相同的key写入一个分区文件中，可能会写入多个不同的分区文件中

Shuffle Read：reduce task就会从上一个stage的所有task所在的机器上
寻找属于自己的的那些分区文件，这样就可以保证每一个key所对应的value都会
会聚到同一个节点上去处理和聚合


--普通机制
HashShuffle过程中可能会产生的问题-buffer大小为32k
1.小文件过多，耗时低效的IO操作
2.OOM，读写文件以及缓存过多

--优化后的HashShuffleManager
每一个核中跑的task共用一份Buffer缓冲区
小文件个数取决于核个数和reduce个数


--SortShuffle运行原理  --version 1.2以后
1.普通机制
2.bypass机制


普通机制过程：
map task处理数据，首先将结果写入内存数据结构中，这个内存数据结构大小初始为5M。
如果map task一直往内存数据结构中写入数据的时候，内存数据结构就会被写满，因此，在
这种机制下面，有一种不定期估算机制去估算内存数据结构的大小，假设下一次写入大小为5.01M，
那么内存数据结构就会满，它会申请该大小的2倍的值，再减去原来的值5M，即5.01*2-5=5.02M，
那么现在的内存数据结构大小为5M+5.02M=10.02M。以此类推，一直估算下去，当申请不到内存的时候，
就会溢写磁盘，在溢写的过程中有排序操作。然后分批写入磁盘文件，每批为1万条数据。最后形成两个磁盘
小文件，即索引文件和磁盘文件，即小文件个数为2*M(map task个数)

为了解决磁盘IO，把HashShuffle换成了SortShuffle


普通机制和bypass机制区别
bypass机制少了排序操作


bypass运行机制的触发条件如下
shuffle reduce task数量小于spark.shuffle.sort.bypassMergeThreshold参数的值（默认为200）

--官网配置
http://spark.apache.org/docs/1.6.3/configuration.html#shuffle-behavior



---------------------------------------------------
--Spark shuffle寻址
MapOutputTracker管理磁盘小文件
主从关系
MapOutputTrackerMaster(Driver)
MapOutputTrackerWorker(Executor)

BlockManager块管理者
主从关系
BlockManagerMaster(Driver)
 DiskStore 管理磁盘数据
 MemeoryStore 管理内存数据
 ConnectionManager 连接其他BlockManager
 BlockTransferService 拉取数据
BlockManagerWorker(Executor)
 DiskStore 管理磁盘数据
 MemeoryStore 管理内存数据
 ConnectionManager 连接其他BlockManager
 BlockTransferService 拉取数据


1.map task处理完数据后，会将数据结果和落地磁盘位置封装到MapStatus对象中，通过MapOutputTrackerWorker汇报
给Driver中的MapOutputTrackerMaster，Driver掌握了磁盘小文件位置
2.Reduce Task拉取数据之前向Driver中的MapOutputTrackerMaster要磁盘小文件的位置，MapOutputTrackerMaster
返回磁盘小文件的位置
3.Reduce Task中的BlockManagerWorker去连接map task中的BlockManagerWorker
4.BlockTransferService默认启动5个子线程拉取数据，默认这个5个task一次拉取的数据量不能超过48M


reduce OOM问题？
1.减少拉取数据量
2.增大Executor总体内存
3.增加shuffle内存比例




---------------------------------------------------
--Spark 内存管理
--参考：http://spark.apache.org/docs/1.6.3/configuration.html#memory-management
主要是指Worker里面Executor的内存管理
Spark在1.6之前使用的是静态内存管理；1.6之后，使用统一内存管理

静态内存管理把Executor分为三部分 配置： spark.memory.useLegacyMode=true
20% - task运行（1-spark.shuffle.memoryFraction-spark.storage.memoryFraction）
20%（spark.shuffle.memoryFraction） - 20%预留内存， 80%shuffle聚合内存
60%（spark.storage.memoryFraction） - 10%预留内存， 90%-20%数据反序列化（spark.storage.unrollFraction）， 80%RDD的缓存和广播变量


Reduce端OOM解决方法
1.减少拉取数据量<48M
2.增大Executor总体内存
3.增加shuffle内存比例



Executor统一内存管理，也分为三部分  配置：spark.memory.useLegacyMode=false   version 1.6以后默认开启同一内存管理
300M预留
(总内存-300M)*0.25 task运行(1-spark.memory.fraction=0.25)

(总内存-300M)*0.75（spark.memory.fraction） - A.(总内存-300M)*0.75*0.5（spark.memory.storageFraction） -RDD的缓存和广播变量
                    B.(总内存-300M)*0.75*0.5 -shuffle聚合内存
					A和B之前内存可以相互借用





---------------------------------------------------
--Spark Shuffle调优
1:sparkconf.set("spark.shuffle.file.buffer","64K") --不建议使用，因为这么写相当于硬编码 --最高
2：在conf/spark-defaults.conf ---不建议使用，相当于硬编码 --第三
3：./spark-submit --conf spark.shuffle.file.buffer=64 --conf spark.reducer.maxSizeInFlight=96 --建议使用 --第二


spark.shuffle.file.buffer
默认值：32k
参数说明：该参数用于设置shuffle write task的BufferedOutputStream的buffer缓冲大小。将数据写到磁盘文件之前，会先写入buffer缓冲中，待缓冲写满之后，才会溢写到磁盘。
调优建议：如果作业可用的内存资源较为充足的话，可以适当增加这个参数的大小（比如64k），从而减少shuffle write过程中溢写磁盘文件的次数，也就可以减少磁盘IO次数，进而提升性能。在实践中发现，合理调节该参数，性能会有1%~5%的提升。
参考：http://spark.apache.org/docs/1.6.3/configuration.html#shuffle-behavior

spark.reducer.maxSizeInFlight
默认值：48m 
参数说明：该参数用于设置shuffle read task的buffer缓冲大小，而这个buffer缓冲决定了每次能够拉取多少数据。
调优建议：如果作业可用的内存资源较为充足的话，可以适当增加这个参数的大小（比如96m），从而减少拉取数据的次数，也就可以减少网络传输的次数，进而提升性能。在实践中发现，合理调节该参数，性能会有1%~5%的提升。
参考：http://spark.apache.org/docs/1.6.3/configuration.html#shuffle-behavior


spark.shuffle.io.maxRetries
默认值：3
参数说明：shuffle read task从shuffle write task所在节点拉取属于自己的数据时，如果因为网络异常导致拉取失败，是会自动进行重试的。该参数就代表了可以重试的最大次数。如果在指定次数之内拉取还是没有成功，就可能会导致作业执行失败。
调优建议：对于那些包含了特别耗时的shuffle操作的作业，建议增加重试最大次数（比如60次），以避免由于JVM的full gc或者网络不稳定等因素导致的数据拉取失败。在实践中发现，对于针对超大数据量（数十亿~上百亿）的shuffle过程，调节该参数可以大幅度提升稳定性。
shuffle file not find    taskScheduler不负责重试task，由DAGScheduler负责重试stage
参考：http://spark.apache.org/docs/1.6.3/configuration.html#shuffle-behavior


spark.shuffle.io.retryWait
默认值：5s
参数说明：具体解释同上，该参数代表了每次重试拉取数据的等待间隔，默认是5s。
调优建议：建议加大间隔时长（比如60s），以增加shuffle操作的稳定性。
参考：http://spark.apache.org/docs/1.6.3/configuration.html#shuffle-behavior


spark.shuffle.memoryFraction
默认值：0.2
参数说明：该参数代表了Executor内存中，分配给shuffle read task进行聚合操作的内存比例，默认是20%。
调优建议：如果内存充足，而且很少使用持久化操作，建议调高这个比例，给shuffle read的聚合操作更多内存，以避免由于内存不足导致聚合过程中频繁读写磁盘。在实践中发现，合理调节该参数可以将性能提升10%左右。
参考：http://spark.apache.org/docs/1.6.3/configuration.html#memory-management

spark.shuffle.manager
默认值：sort|hash
参数说明：该参数用于设置ShuffleManager的类型。Spark 1.5以后，有三个可选项：hash、sort和tungsten-sort。HashShuffleManager是Spark 1.2以前的默认选项，但是Spark 1.2以及之后的版本默认都是SortShuffleManager了。tungsten-sort与sort类似，但是使用了tungsten计划中的堆外内存管理机制，内存使用效率更高。
调优建议：由于SortShuffleManager默认会对数据进行排序，因此如果你的业务逻辑中需要该排序机制的话，则使用默认的SortShuffleManager就可以；而如果你的业务逻辑不需要对数据进行排序，那么建议参考后面的几个参数调优，通过bypass机制或优化的HashShuffleManager来避免排序操作，同时提供较好的磁盘读写性能。这里要注意的是，tungsten-sort要慎用，因为之前发现了一些相应的bug。
参考：http://spark.apache.org/docs/1.6.3/configuration.html#shuffle-behavior

spark.shuffle.sort.bypassMergeThreshold----针对SortShuffle
默认值：200
参数说明：当ShuffleManager为SortShuffleManager时，如果shuffle read task的数量小于这个阈值（默认是200），则shuffle write过程中不会进行排序操作，而是直接按照未经优化的HashShuffleManager的方式去写数据，但是最后会将每个task产生的所有临时磁盘文件都合并成一个文件，并会创建单独的索引文件。
调优建议：当你使用SortShuffleManager时，如果的确不需要排序操作，那么建议将这个参数调大一些，大于shuffle read task的数量。那么此时就会自动启用bypass机制，map-side就不会进行排序了，减少了排序的性能开销。但是这种方式下，依然会产生大量的磁盘文件，因此shuffle write性能有待提高。
参考：http://spark.apache.org/docs/1.6.3/configuration.html#shuffle-behavior

spark.shuffle.consolidateFiles----针对HashShuffle
默认值：false
参数说明：如果使用HashShuffleManager，该参数有效。如果设置为true，那么就会开启consolidate机制，会大幅度合并shuffle write的输出文件，对于shuffle read task数量特别多的情况下，这种方法可以极大地减少磁盘IO开销，提升性能。
调优建议：如果的确不需要SortShuffleManager的排序机制，那么除了使用bypass机制，还可以尝试将spark.shffle.manager参数手动指定为hash，使用HashShuffleManager，同时开启consolidate机制。在实践中尝试过，发现其性能比开启了bypass机制的SortShuffleManager要高出10%~30%。







---------------------------------------------------
--广播变量
1.不能讲RDD广播出去，可以将RDD的结果广播出去
2.广播变量只能在Driver定义，在Executor端使用，不能在Executor端改变
3.如果不使用广播变量，在一个Executor中有多少个task，就有多少个变量副本，如果使用广播变量，在每个Executor中只有一份Driver端的广播变量副本

















