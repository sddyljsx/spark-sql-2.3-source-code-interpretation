spark sql 2.3 源码解读 - 架构概览 (1)

​    spark sql 的前身是shark，类似于 hive， 用户可以基于spark引擎使用sql语句对数据进行分析，而不用去编写程序代码。

![屏幕快照 2018-08-12 下午2.55.58](https://ws1.sinaimg.cn/large/006tNbRwly1fu6xlztqzyj30ni0ec40y.jpg)

​    上图很好的展示了spark sql的功能，提供了 jdbc，console，编程接口三种方式来操作RDD(Resilient Distributed Datasets)，用户只需要编写sql即可，不需要编写程序代码。

​    spark sql的运行流程如下：

![屏幕快照 2018-08-12 下午2.56.38](https://ws1.sinaimg.cn/large/006tNbRwly1fu6y90emdkj31i20dogq7.jpg)

  大概有6步：

1. sql 语句经过 SqlParser 解析成 Unresolved Logical Plan;
2. analyzer 结合 catalog 进行绑定,生成 Logical Plan;
3. optimizer 对 Logical Plan 优化,生成 Optimized LogicalPlan;
4. SparkPlan 将 Optimized LogicalPlan 转换成 Physical Plan;
5. prepareForExecution()将 Physical Plan 转换成 executed Physical Plan;
6. execute()执行可执行物理计划，得到RDD; 

上述流程在spark中对应的源码部分：

```
class QueryExecution(val sparkSession: SparkSession, val logical: LogicalPlan) {

  // TODO: Move the planner an optimizer into here from SessionState.
  protected def planner = sparkSession.sessionState.planner

  def assertAnalyzed(): Unit = analyzed

  def assertSupported(): Unit = {
    if (sparkSession.sessionState.conf.isUnsupportedOperationCheckEnabled) {
      UnsupportedOperationChecker.checkForBatch(analyzed)
    }
  }

  lazy val analyzed: LogicalPlan = {
    SparkSession.setActiveSession(sparkSession)
    sparkSession.sessionState.analyzer.executeAndCheck(logical)
  }

  lazy val withCachedData: LogicalPlan = {
    assertAnalyzed()
    assertSupported()
    sparkSession.sharedState.cacheManager.useCachedData(analyzed)
  }

  lazy val optimizedPlan: LogicalPlan = {
    sparkSession.sessionState.optimizer.execute(withCachedData)
  }

  lazy val sparkPlan: SparkPlan = {
    SparkSession.setActiveSession(sparkSession)
    // TODO: We use next(), i.e. take the first plan returned by the planner, here for now,
    //       but we will implement to choose the best plan.
    planner.plan(ReturnAnswer(optimizedPlan)).next()
  }

  // executedPlan should not be used to initialize any SparkPlan. It should be
  // only used for execution.
  lazy val executedPlan: SparkPlan = prepareForExecution(sparkPlan)

  /** Internal version of the RDD. Avoids copies and has no schema */
  lazy val toRdd: RDD[InternalRow] = executedPlan.execute()
```

​     逻辑交代的非常清楚，从最后一行的   lazy val toRdd: RDD[InternalRow] = executedPlan.execute() 往前推便可清晰看到整个流程。细心的同学也可以看到 所有步骤都是lazy的，只有调用了execute才会触发执行，这也是spark的重要设计思想。

​     后面的文章将对这些流程进行详细的介绍。

 

 

 