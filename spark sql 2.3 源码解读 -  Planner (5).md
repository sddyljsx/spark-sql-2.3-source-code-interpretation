spark sql 2.3 源码解读 -  Planner (5)

Optimizer得到的是最终的 Logical Plan，Planner 将Logical Plan 转化为 Physical Plan。

planner执行plan方法：

```
lazy val sparkPlan: SparkPlan = {
  SparkSession.setActiveSession(sparkSession)
  // TODO: We use next(), i.e. take the first plan returned by the planner, here for now,
  //       but we will implement to choose the best plan.
  // 在一系列plan中选取一个
  planner.plan(ReturnAnswer(optimizedPlan)).next()
}
```

QueryPlanner的plan，生成一系列plan：

```
def plan(plan: LogicalPlan): Iterator[PhysicalPlan] = {
  // Obviously a lot to do here still...

  // Collect physical plan candidates.
  // strategies 实现了转化
  val candidates = strategies.iterator.flatMap(_(plan))

  // The candidates may contain placeholders marked as [[planLater]],
  // so try to replace them by their child plans.
  // 移除planLater
  val plans = candidates.flatMap { candidate =>
    val placeholders = collectPlaceholders(candidate)

    if (placeholders.isEmpty) {
      // Take the candidate as is because it does not contain placeholders.
      Iterator(candidate)
    } else {
      // Plan the logical plan marked as [[planLater]] and replace the placeholders.
      placeholders.iterator.foldLeft(Iterator(candidate)) {
        case (candidatesWithPlaceholders, (placeholder, logicalPlan)) =>
          // Plan the logical plan for the placeholder.
          val childPlans = this.plan(logicalPlan)

          candidatesWithPlaceholders.flatMap { candidateWithPlaceholders =>
            childPlans.map { childPlan =>
              // Replace the placeholder by the child plan
              candidateWithPlaceholders.transformUp {
                case p if p == placeholder => childPlan
              }
            }
          }
      }
    }
  }
  // 裁剪plan，去掉 bad plan，但目前只是原封不动返回
  val pruned = prunePlans(plans)
  assert(pruned.hasNext, s"No plan for $plan")
  pruned
}
```
下面看一下 strategies：
```
override def strategies: Seq[Strategy] =
  experimentalMethods.extraStrategies ++
    extraPlanningStrategies ++ (
    DataSourceV2Strategy ::
    FileSourceStrategy ::
    DataSourceStrategy(conf) ::
    SpecialLimits ::
    Aggregation ::
    JoinSelection ::
    InMemoryScans ::
    BasicOperators :: Nil)
```
Strategy输入一个LogicalPlan，输出a list of PhysicalPlan
```
/**
 * Given a [[LogicalPlan]], returns a list of `PhysicalPlan`s that can
 * be used for execution. If this strategy does not apply to the given logical operation then an
 * empty list should be returned.
 */
abstract class GenericStrategy[PhysicalPlan <: TreeNode[PhysicalPlan]] extends Logging {

  /**
   * Returns a placeholder for a physical plan that executes `plan`. This placeholder will be
   * filled in automatically by the QueryPlanner using the other execution strategies that are
   * available.
   */
  protected def planLater(plan: LogicalPlan): PhysicalPlan

  def apply(plan: LogicalPlan): Seq[PhysicalPlan]
}
```
以前面的 SELECT A.B FROM A 为例：

```
val df = spark.read.json("examples/src/main/resources/test.json")
df.createOrReplaceTempView("A")
val result = spark.sql("SELECT A.B FROM A")
```

因为是从文件中读取，所以最终match的 strategy为：

```
object FileSourceStrategy extends Strategy with Logging {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projects, filters,
      l @ LogicalRelation(fsRelation: HadoopFsRelation, _, table, _)) =>
      // Filters on this relation fall into four categories based on where we can use them to avoid
      // reading unneeded data:
      //  - partition keys only - used to prune directories to read
      //  - bucket keys only - optionally used to prune files to read
      //  - keys stored in the data only - optionally used to skip groups of data in files
      //  - filters that need to be evaluated again after the scan
      val filterSet = ExpressionSet(filters)

      // The attribute name of predicate could be different than the one in schema in case of
      // case insensitive, we should change them to match the one in schema, so we do not need to
      // worry about case sensitivity anymore.
      val normalizedFilters = filters.map { e =>
        e transform {
          case a: AttributeReference =>
            a.withName(l.output.find(_.semanticEquals(a)).get.name)
        }
      }

      val partitionColumns =
        l.resolve(
          fsRelation.partitionSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)
      val partitionSet = AttributeSet(partitionColumns)
      val partitionKeyFilters =
        ExpressionSet(normalizedFilters
          .filterNot(SubqueryExpression.hasSubquery(_))
          .filter(_.references.subsetOf(partitionSet)))

      logInfo(s"Pruning directories with: ${partitionKeyFilters.mkString(",")}")

      val dataColumns =
        l.resolve(fsRelation.dataSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)

      // Partition keys are not available in the statistics of the files.
      val dataFilters = normalizedFilters.filter(_.references.intersect(partitionSet).isEmpty)

      // Predicates with both partition keys and attributes need to be evaluated after the scan.
      val afterScanFilters = filterSet -- partitionKeyFilters.filter(_.references.nonEmpty)
      logInfo(s"Post-Scan Filters: ${afterScanFilters.mkString(",")}")

      val filterAttributes = AttributeSet(afterScanFilters)
      val requiredExpressions: Seq[NamedExpression] = filterAttributes.toSeq ++ projects
      val requiredAttributes = AttributeSet(requiredExpressions)

      val readDataColumns =
        dataColumns
          .filter(requiredAttributes.contains)
          .filterNot(partitionColumns.contains)
      val outputSchema = readDataColumns.toStructType
      logInfo(s"Output Data Schema: ${outputSchema.simpleString(5)}")

      val outputAttributes = readDataColumns ++ partitionColumns
	  // 最终结果
      val scan =
        FileSourceScanExec(
          fsRelation,
          outputAttributes,
          outputSchema,
          partitionKeyFilters.toSeq,
          dataFilters,
          table.map(_.identifier))

      val afterScanFilter = afterScanFilters.toSeq.reduceOption(expressions.And)
      val withFilter = afterScanFilter.map(execution.FilterExec(_, scan)).getOrElse(scan)
      val withProjections = if (projects == withFilter.output) {
        withFilter
      } else {
        execution.ProjectExec(projects, withFilter)
      }

      withProjections :: Nil

    case _ => Nil
  }
}
```
```
case class FileSourceScanExec(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier])
  extends DataSourceScanExec with ColumnarBatchScan  {
```

最终结果：

```
`FileScan json [B#6] Batched: false, Format: JSON, Location: InMemoryFileIndex[file:/Users/neal/github/spark2.3/spark/examples/src/main/resources/test.json], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<B:string>`
```
这便是一个可执行的Physical Plan了。