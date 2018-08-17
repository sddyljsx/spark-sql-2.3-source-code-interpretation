spark sql 2.3 源码解读 -  Analyzer (3.1)

本章将介绍analyzer 结合 catalog 进行绑定,生成 Resolved Logical Plan.

上一步得到的 Unresolved Logical Plan将会执行：

```
lazy val analyzed: LogicalPlan = {
  SparkSession.setActiveSession(sparkSession)
  sparkSession.sessionState.analyzer.executeAndCheck(logical)
}
```
Analyzer源码：
```
/**
 * Provides a logical query plan analyzer, which translates [[UnresolvedAttribute]]s and
 * [[UnresolvedRelation]]s into fully typed objects using information in a [[SessionCatalog]].
 */
class Analyzer(
    catalog: SessionCatalog,
    conf: SQLConf,
    maxIterations: Int)
  extends RuleExecutor[LogicalPlan] with CheckAnalysis {
  
  def executeAndCheck(plan: LogicalPlan): LogicalPlan = {
    // 执行 analyze逻辑
    val analyzed = execute(plan)
    try {
      checkAnalysis(analyzed)
      EliminateBarriers(analyzed)
    } catch {
      case e: AnalysisException =>
        val ae = new AnalysisException(e.message, e.line, e.startPosition, Option(analyzed))
        ae.setStackTrace(e.getStackTrace)
        throw ae
    }
  }

  override def execute(plan: LogicalPlan): LogicalPlan = {
    AnalysisContext.reset()
    try {
      executeSameContext(plan)
    } finally {
      AnalysisContext.reset()
    }
  }
  
 private def executeSameContext(plan: LogicalPlan): LogicalPlan = super.execute(plan)
```
Analyzer继承自RuleExecutor[LogicalPlan]，而执行的关键函数调用的是super.execute(plan)方法，所以我们先看一下RuleExecutor，他做的事情很简单，就是按批次，按顺序对plan执行rule，会迭代多次。下面代码逻辑很简单，只是log比较多而已。

```
abstract class RuleExecutor[TreeType <: TreeNode[_]] extends Logging {
  // 定义了两种策略，一次和固定次数
  /**
   * An execution strategy for rules that indicates the maximum number of executions. If the
   * execution reaches fix point (i.e. converge) before maxIterations, it will stop.
   */
  abstract class Strategy { def maxIterations: Int }

  /** A strategy that only runs once. */
  case object Once extends Strategy { val maxIterations = 1 }

  /** A strategy that runs until fix point or maxIterations times, whichever comes first. */
  case class FixedPoint(maxIterations: Int) extends Strategy
  //  一个批次的rules
  /** A batch of rules. */
  protected case class Batch(name: String, strategy: Strategy, rules: Rule[TreeType]*)
  //  所有的rule，按批次存放
  /** Defines a sequence of rule batches, to be overridden by the implementation. */
  protected def batches: Seq[Batch]
  // 执行rule，关键代码很简单，就是按批次，按顺序对plan执行rule，会迭代多次
  /**
   * Executes the batches of rules defined by the subclass. The batches are executed serially
   * using the defined execution strategy. Within each batch, rules are also executed serially.
   */
  def execute(plan: TreeType): TreeType = {
    var curPlan = plan
    val queryExecutionMetrics = RuleExecutor.queryExecutionMeter

    batches.foreach { batch =>
      val batchStartPlan = curPlan
      var iteration = 1
      var lastPlan = curPlan
      var continue = true

      // Run until fix point (or the max number of iterations as specified in the strategy.
      while (continue) {
        curPlan = batch.rules.foldLeft(curPlan) {
          case (plan, rule) =>
            val startTime = System.nanoTime()
            // 执行rule，得到新的plan
            val result = rule(plan)
            val runTime = System.nanoTime() - startTime
            // 判断rule是否起了作用
            if (!result.fastEquals(plan)) {
              queryExecutionMetrics.incNumEffectiveExecution(rule.ruleName)
              queryExecutionMetrics.incTimeEffectiveExecutionBy(rule.ruleName, runTime)
              logTrace(
                s"""
                  |=== Applying Rule ${rule.ruleName} ===
                  |${sideBySide(plan.treeString, result.treeString).mkString("\n")}
                """.stripMargin)
            }
            queryExecutionMetrics.incExecutionTimeBy(rule.ruleName, runTime)
            queryExecutionMetrics.incNumExecution(rule.ruleName)

            // Run the structural integrity checker against the plan after each rule.
            if (!isPlanIntegral(result)) {
              val message = s"After applying rule ${rule.ruleName} in batch ${batch.name}, " +
                "the structural integrity of the plan is broken."
              throw new TreeNodeException(result, message, null)
            }

            result
        }
        // 是否达到迭代次数
        iteration += 1
        if (iteration > batch.strategy.maxIterations) {
          // Only log if this is a rule that is supposed to run more than once.
          if (iteration != 2) {
            val message = s"Max iterations (${iteration - 1}) reached for batch ${batch.name}"
            if (Utils.isTesting) {
              throw new TreeNodeException(curPlan, message, null)
            } else {
              logWarning(message)
            }
          }
          continue = false
        }

        if (curPlan.fastEquals(lastPlan)) {
          logTrace(
            s"Fixed point reached for batch ${batch.name} after ${iteration - 1} iterations.")
          continue = false
        }
        lastPlan = curPlan
      }
      // 该批次rule是否起作用
      if (!batchStartPlan.fastEquals(curPlan)) {
        logDebug(
          s"""
            |=== Result of Batch ${batch.name} ===
            |${sideBySide(batchStartPlan.treeString, curPlan.treeString).mkString("\n")}
          """.stripMargin)
      } else {
        logTrace(s"Batch ${batch.name} has no effect.")
      }
    }

    curPlan
  }
}
```

如果对scala的foldLeft不熟悉，可以看这里: https://blog.csdn.net/oopsoom/article/details/23447317

foldLeft在spark源码中使用的很广泛，一定要搞懂。

再看rule：

```
abstract class Rule[TreeType <: TreeNode[_]] extends Logging {

  /** Name for this rule, automatically inferred based on class name. */
  val ruleName: String = {
    val className = getClass.getName
    if (className endsWith "$") className.dropRight(1) else className
  }

  def apply(plan: TreeType): TreeType
}
```

输入为旧的plan，输出为新的plan，仅此而已。所以真正的逻辑在各个继承实现的rule里，analyze的过程也就是执行各个rule的过程。下一节会详细讲解。

这里的Rule和RuleExecutor不仅仅在这里使用，在后面的Optimizer等都有使用。