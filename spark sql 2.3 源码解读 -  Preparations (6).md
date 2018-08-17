spark sql 2.3 源码解读 -  Preparations (6)

上一章生成的Physical Plan 还需要经过prepareForExecution这一步，做执行前的一些准备工作，代码如下：

```
/ executedPlan should not be used to initialize any SparkPlan. It should be
// only used for execution.
lazy val executedPlan: SparkPlan = prepareForExecution(sparkPlan)
```

```
/**
  * Prepares a planned [[SparkPlan]] for execution by inserting shuffle operations and internal
  * row format conversions as needed.
  */
protected def prepareForExecution(plan: SparkPlan): SparkPlan = {
  preparations.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
}
```

看到上面的 foldLeft 是不是很熟悉，大家应该明白和前面是一个套路了，就是将规则遍历应用到plan。

直接看preparations，注释也写的很清楚：

```
/** A sequence of rules that will be applied in order to the physical plan before execution. */
protected def preparations: Seq[Rule[SparkPlan]] = Seq(
  python.ExtractPythonUDFs,
  PlanSubqueries(sparkSession),
  EnsureRequirements(sparkSession.sessionState.conf),
  CollapseCodegenStages(sparkSession.sessionState.conf),
  ReuseExchange(sparkSession.sessionState.conf),
  ReuseSubquery(sparkSession.sessionState.conf))
```

rule并不是很多，其中EnsureRequirements是很重要的，下面着重讲一下：

apply 入口：

```
def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
  // TODO: remove this after we create a physical operator for `RepartitionByExpression`.
  case operator @ ShuffleExchangeExec(upper: HashPartitioning, child, _) =>
    child.outputPartitioning match {
      case lower: HashPartitioning if upper.semanticEquals(lower) => child
      case _ => operator
    }
  case operator: SparkPlan =>
    // 执行 ensureDistributionAndOrdering
    ensureDistributionAndOrdering(reorderJoinPredicates(operator))
}
```

```
private def ensureDistributionAndOrdering(operator: SparkPlan): SparkPlan = {
  val requiredChildDistributions: Seq[Distribution] = operator.requiredChildDistribution
  val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering
  var children: Seq[SparkPlan] = operator.children
  assert(requiredChildDistributions.length == children.length)
  assert(requiredChildOrderings.length == children.length)

  // Ensure that the operator's children satisfy their output distribution requirements.
  // children的实际输出分布(其实就是partitioning)满足要求的输出分布
  children = children.zip(requiredChildDistributions).map {
   //满足，直接返回
    case (child, distribution) if child.outputPartitioning.satisfies(distribution) =>
      child
    // 广播单独处理
    case (child, BroadcastDistribution(mode)) =>
      BroadcastExchangeExec(mode, child)
    case (child, distribution) =>
      val numPartitions = distribution.requiredNumPartitions
        .getOrElse(defaultNumPreShufflePartitions)
      // 做一次shuffle(可以认为是重新分区,改变分布)，满足需求。 
      ShuffleExchangeExec(distribution.createPartitioning(numPartitions), child)
  }

  // Get the indexes of children which have specified distribution requirements and need to have
  // same number of partitions.
  // 过滤有指定分布需求的children
  val childrenIndexes = requiredChildDistributions.zipWithIndex.filter {
    case (UnspecifiedDistribution, _) => false
    case (_: BroadcastDistribution, _) => false
    case _ => true
  }.map(_._2)
  // children 的 partition 数量
  val childrenNumPartitions =
    childrenIndexes.map(children(_).outputPartitioning.numPartitions).toSet

  if (childrenNumPartitions.size > 1) {
    // Get the number of partitions which is explicitly required by the distributions.
    // children 的分布需要的partition数量
    val requiredNumPartitions = {
      val numPartitionsSet = childrenIndexes.flatMap {
        index => requiredChildDistributions(index).requiredNumPartitions
      }.toSet
      assert(numPartitionsSet.size <= 1,
        s"$operator have incompatible requirements of the number of partitions for its children")
      numPartitionsSet.headOption
    }
    // partition 数量目标
    val targetNumPartitions = requiredNumPartitions.getOrElse(childrenNumPartitions.max)
    // 指定分布的children的partition数量全部统一为 targetNumPartitions
    children = children.zip(requiredChildDistributions).zipWithIndex.map {
      case ((child, distribution), index) if childrenIndexes.contains(index) =>
        if (child.outputPartitioning.numPartitions == targetNumPartitions) {
          // 符合目标，直接返回
          child
        } else {
         // 不符合目标，shuffle
          val defaultPartitioning = distribution.createPartitioning(targetNumPartitions)
          child match {
            // If child is an exchange, we replace it with a new one having defaultPartitioning.
            case ShuffleExchangeExec(_, c, _) => ShuffleExchangeExec(defaultPartitioning, c)
            case _ => ShuffleExchangeExec(defaultPartitioning, child)
          }
        }

      case ((child, _), _) => child
    }
  }

  // 添加ExchangeCoordinator，这个单独讲
  // Now, we need to add ExchangeCoordinator if necessary.
  // Actually, it is not a good idea to add ExchangeCoordinators while we are adding Exchanges.
  // However, with the way that we plan the query, we do not have a place where we have a
  // global picture of all shuffle dependencies of a post-shuffle stage. So, we add coordinator
  // at here for now.
  // Once we finish https://issues.apache.org/jira/browse/SPARK-10665,
  // we can first add Exchanges and then add coordinator once we have a DAG of query fragments.
  children = withExchangeCoordinator(children, requiredChildDistributions)

  // 如果有sort的需求，则加上SortExec
  // Now that we've performed any necessary shuffles, add sorts to guarantee output orderings:
  children = children.zip(requiredChildOrderings).map { case (child, requiredOrdering) =>
    // If child.outputOrdering already satisfies the requiredOrdering, we do not need to sort.
    if (SortOrder.orderingSatisfies(child.outputOrdering, requiredOrdering)) {
      child
    } else {
      SortExec(requiredOrdering, global = false, child = child)
    }
  }

  operator.withNewChildren(children)
}
```

withExchangeCoordinator的逻辑如下,他用来调节多个spark plan的数据分布：

```
/**
 * Adds [[ExchangeCoordinator]] to [[ShuffleExchangeExec]]s if adaptive query execution is enabled
 * and partitioning schemes of these [[ShuffleExchangeExec]]s support [[ExchangeCoordinator]].
 */
private def withExchangeCoordinator(
    children: Seq[SparkPlan],
    requiredChildDistributions: Seq[Distribution]): Seq[SparkPlan] = {
  // 判断是否需要添加 ExchangeCoordinator
  val supportsCoordinator =
    if (children.exists(_.isInstanceOf[ShuffleExchangeExec])) {
      // Right now, ExchangeCoordinator only support HashPartitionings.
      children.forall {
       // 条件1 children中有ShuffleExchangeExec且分区为HashPartitioning
        case e @ ShuffleExchangeExec(hash: HashPartitioning, _, _) => true
        case child =>
          child.outputPartitioning match {
            case hash: HashPartitioning => true
            case collection: PartitioningCollection =>
              collection.partitionings.forall(_.isInstanceOf[HashPartitioning])
            case _ => false
          }
      }
    } else {
      // In this case, although we do not have Exchange operators, we may still need to
      // shuffle data when we have more than one children because data generated by
      // these children may not be partitioned in the same way.
      // Please see the comment in withCoordinator for more details.
      // 条件2 分布为ClusteredDistribution 或 HashClusteredDistribution
      val supportsDistribution = requiredChildDistributions.forall { dist =>
        dist.isInstanceOf[ClusteredDistribution] || dist.isInstanceOf[HashClusteredDistribution]
      }
      children.length > 1 && supportsDistribution
    }

  val withCoordinator =
    // adaptiveExecutionEnabled 且 符合条件1或2
    if (adaptiveExecutionEnabled && supportsCoordinator) {
      val coordinator =
        new ExchangeCoordinator(
          children.length,
          targetPostShuffleInputSize,
          minNumPostShufflePartitions)
      children.zip(requiredChildDistributions).map {
        case (e: ShuffleExchangeExec, _) =>
          // 条件1， 直接添加 coordinator
          // This child is an Exchange, we need to add the coordinator.
          e.copy(coordinator = Some(coordinator))
        case (child, distribution) =>
          // If this child is not an Exchange, we need to add an Exchange for now.
          // Ideally, we can try to avoid this Exchange. However, when we reach here,
          // there are at least two children operators (because if there is a single child
          // and we can avoid Exchange, supportsCoordinator will be false and we
          // will not reach here.). Although we can make two children have the same number of
          // post-shuffle partitions. Their numbers of pre-shuffle partitions may be different.
          // For example, let's say we have the following plan
          //         Join
          //         /  \
          //       Agg  Exchange
          //       /      \
          //    Exchange  t2
          //      /
          //     t1
          // In this case, because a post-shuffle partition can include multiple pre-shuffle
          // partitions, a HashPartitioning will not be strictly partitioned by the hashcodes
          // after shuffle. So, even we can use the child Exchange operator of the Join to
          // have a number of post-shuffle partitions that matches the number of partitions of
          // Agg, we cannot say these two children are partitioned in the same way.
          // Here is another case
          //         Join
          //         /  \
          //       Agg1  Agg2
          //       /      \
          //   Exchange1  Exchange2
          //       /       \
          //      t1       t2
          // In this case, two Aggs shuffle data with the same column of the join condition.
          // After we use ExchangeCoordinator, these two Aggs may not be partitioned in the same
          // way. Let's say that Agg1 and Agg2 both have 5 pre-shuffle partitions and 2
          // post-shuffle partitions. It is possible that Agg1 fetches those pre-shuffle
          // partitions by using a partitionStartIndices [0, 3]. However, Agg2 may fetch its
          // pre-shuffle partitions by using another partitionStartIndices [0, 4].
          // So, Agg1 and Agg2 are actually not co-partitioned.
          //
          // It will be great to introduce a new Partitioning to represent the post-shuffle
          // partitions when one post-shuffle partition includes multiple pre-shuffle partitions.
          // 条件2，这个child的children虽然分区数目一样，但是不一定是同一种分区方式，所以加上coordinator
          val targetPartitioning = distribution.createPartitioning(defaultNumPreShufflePartitions)
          assert(targetPartitioning.isInstanceOf[HashPartitioning])
          ShuffleExchangeExec(targetPartitioning, child, Some(coordinator))
      }
    } else {
      // If we do not need ExchangeCoordinator, the original children are returned.
      children
    }

  withCoordinator
}
```
如下所示，adaptiveExecutionEnabled 默认是false的，所以ExchangeCoordinator默认是关闭的，等下一章我们会详细讲解它的执行逻辑，在这里大家先有个印象就好。
```
`val ADAPTIVE_EXECUTION_ENABLED = buildConf("spark.sql.adaptive.enabled")`
    .doc("When true, enable adaptive query execution.")
    .booleanConf
    `.createWithDefault(false)`
```


总结一下，Preparations 会比较 children的实际输出分布和需求输出分布的不同(比较partitioning)，然后添加ShuffleExchangeExec、ExchangeCoordinator、SortExec做转化处理，使其匹配。

下面给一个例子，代码如下：

```
val df = spark.read.json("examples/src/main/resources/test.json")
df.createOrReplaceTempView("A")
spark.sql("SELECT A.B FROM A ORDER BY A.B").collectAsList()
```
执行到prepare时断点调试一下：

![屏幕快照 2018-08-15 下午3.40.56](https://ws2.sinaimg.cn/large/006tNbRwly1fuafsv7qidj31gg09242k.jpg)

requireChildDistribution 是 OrderedDistribution, 但 child的outputPartitioning是null，所以会触发添加ShuffleExchangeExec的逻辑。

OrderedDistribution代码如下，会创建RangePartitioning：


```
case class OrderedDistribution(ordering: Seq[SortOrder]) extends Distribution {
  require(
    ordering != Nil,
    "The ordering expressions of an OrderedDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")

  override def requiredNumPartitions: Option[Int] = None

  override def createPartitioning(numPartitions: Int): Partitioning = {
    RangePartitioning(ordering, numPartitions)
  }
}
```

执行前后结果对比，和预期相符合：

执行前：
```
Sort [B#6 ASC NULLS FIRST], true, 0
+- FileScan json [B#6] Batched: false, Format: JSON, Location: InMemoryFileIndex[file:/Users/neal/github/spark2.3/spark/examples/src/main/resources/test.json], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<B:string>
```
执行后：
```
*(2) Sort [B#6 ASC NULLS FIRST], true, 0
+- Exchange rangepartitioning(B#6 ASC NULLS FIRST, 200)
   +- *(1) FileScan json [B#6] Batched: false, Format: JSON, Location: InMemoryFileIndex[file:/Users/neal/github/spark2.3/spark/examples/src/main/resources/test.json], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<B:string>
```