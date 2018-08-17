spark sql 2.3 源码解读 -  Optimizer (4)

得到 Resolved Logical Plan 后，将进入优化阶段。后续执行逻辑如下：

```
// 如果缓存中有查询结果，则直接替换为缓存的结果，逻辑不复杂，这里不再展开讲了。
lazy val withCachedData: LogicalPlan = {
  assertAnalyzed()
  assertSupported()
  sparkSession.sharedState.cacheManager.useCachedData(analyzed)
}
// 对Logical Plan 优化
lazy val optimizedPlan: LogicalPlan = {
  sparkSession.sessionState.optimizer.execute(withCachedData)
}
```

下面看一下Optimizer：

```
/**
 * Abstract class all optimizers should inherit of, contains the standard batches (extending
 * Optimizers can override this.
 */
abstract class Optimizer(sessionCatalog: SessionCatalog)
  extends RuleExecutor[LogicalPlan] {
```

看到Optimizer也是继承自RuleExecutor，我们就开心了，和Analyzer一个套路，也是遍历tree，并对每个节点应用rule。下面直接看rules就好了：

```
def batches: Seq[Batch] = {
  val operatorOptimizationRuleSet =
    Seq(
      // Operator push down
      PushProjectionThroughUnion,
      ReorderJoin,
      EliminateOuterJoin,
      PushPredicateThroughJoin,
      PushDownPredicate,
      LimitPushDown,
      ColumnPruning,
      InferFiltersFromConstraints,
      // Operator combine
      CollapseRepartition,
      CollapseProject,
      CollapseWindow,
      CombineFilters,
      CombineLimits,
      CombineUnions,
      // Constant folding and strength reduction
      NullPropagation,
      ConstantPropagation,
      FoldablePropagation,
      OptimizeIn,
      ConstantFolding,
      ReorderAssociativeOperator,
      LikeSimplification,
      BooleanSimplification,
      SimplifyConditionals,
      RemoveDispensableExpressions,
      SimplifyBinaryComparison,
      PruneFilters,
      EliminateSorts,
      SimplifyCasts,
      SimplifyCaseConversionExpressions,
      RewriteCorrelatedScalarSubquery,
      EliminateSerialization,
      RemoveRedundantAliases,
      RemoveRedundantProject,
      SimplifyCreateStructOps,
      SimplifyCreateArrayOps,
      SimplifyCreateMapOps,
      CombineConcats) ++
      extendedOperatorOptimizationRules

  val operatorOptimizationBatch: Seq[Batch] = {
    val rulesWithoutInferFiltersFromConstraints =
      operatorOptimizationRuleSet.filterNot(_ == InferFiltersFromConstraints)
    Batch("Operator Optimization before Inferring Filters", fixedPoint,
      rulesWithoutInferFiltersFromConstraints: _*) ::
    Batch("Infer Filters", Once,
      InferFiltersFromConstraints) ::
    Batch("Operator Optimization after Inferring Filters", fixedPoint,
      rulesWithoutInferFiltersFromConstraints: _*) :: Nil
  }

  (Batch("Eliminate Distinct", Once, EliminateDistinct) ::
  // Technically some of the rules in Finish Analysis are not optimizer rules and belong more
  // in the analyzer, because they are needed for correctness (e.g. ComputeCurrentTime).
  // However, because we also use the analyzer to canonicalized queries (for view definition),
  // we do not eliminate subqueries or compute current time in the analyzer.
  Batch("Finish Analysis", Once,
    EliminateSubqueryAliases,
    EliminateView,
    ReplaceExpressions,
    ComputeCurrentTime,
    GetCurrentDatabase(sessionCatalog),
    RewriteDistinctAggregates,
    ReplaceDeduplicateWithAggregate) ::
  //////////////////////////////////////////////////////////////////////////////////////////
  // Optimizer rules start here
  //////////////////////////////////////////////////////////////////////////////////////////
  // - Do the first call of CombineUnions before starting the major Optimizer rules,
  //   since it can reduce the number of iteration and the other rules could add/move
  //   extra operators between two adjacent Union operators.
  // - Call CombineUnions again in Batch("Operator Optimizations"),
  //   since the other rules might make two separate Unions operators adjacent.
  Batch("Union", Once,
    CombineUnions) ::
  Batch("Pullup Correlated Expressions", Once,
    PullupCorrelatedPredicates) ::
  Batch("Subquery", Once,
    OptimizeSubqueries) ::
  Batch("Replace Operators", fixedPoint,
    ReplaceIntersectWithSemiJoin,
    ReplaceExceptWithFilter,
    ReplaceExceptWithAntiJoin,
    ReplaceDistinctWithAggregate) ::
  Batch("Aggregate", fixedPoint,
    RemoveLiteralFromGroupExpressions,
    RemoveRepetitionFromGroupExpressions) :: Nil ++
  operatorOptimizationBatch) :+
  Batch("Join Reorder", Once,
    CostBasedJoinReorder) :+
  Batch("Decimal Optimizations", fixedPoint,
    DecimalAggregates) :+
  Batch("Object Expressions Optimization", fixedPoint,
    EliminateMapObjects,
    CombineTypedFilters) :+
  Batch("LocalRelation", fixedPoint,
    ConvertToLocalRelation,
    PropagateEmptyRelation) :+
  // The following batch should be executed after batch "Join Reorder" and "LocalRelation".
  Batch("Check Cartesian Products", Once,
    CheckCartesianProducts) :+
  Batch("RewriteSubquery", Once,
    RewritePredicateSubquery,
    ColumnPruning,
    CollapseProject,
    RemoveRedundantProject)
}
```

优化的rule很多，需要sql优化经验才能看懂了。

咱们以sql中最常见的优化谓词下推为例，谓词下推的介绍可以看这里：https://cloud.tencent.com/developer/article/1005925

执行的sql为：`"SELECT A1.B FROM A1 JOIN A2 ON A1.B = A2.B WHERE A1.B = 'Andy'"`

优化前：

```
`Project [B#6]`
`+- Filter (B#6 = Andy)`
   +- Join Inner, (B#6 = B#8)
      :- SubqueryAlias a1
      :  +- Relation[B#6] json
      +- SubqueryAlias a2
         `+- Relation[B#8] json`
```

优化后：

```
`Project [B#6]`
`+- Join Inner, (B#6 = B#8)`
   :- Filter (isnotnull(B#6) && (B#6 = Andy))
   :  +- Relation[B#6] json
   +- Filter (isnotnull(B#8) && (B#8 = Andy))
      `+- Relation[B#8] json`
```
明显可以看到Filter的下推优化。起作用的rule是PushPredicateThroughJoin和InferFiltersFromConstraints

下面着重看一下PushPredicateThroughJoin的关键代码：

```
def apply(plan: LogicalPlan): LogicalPlan = plan transform {
  // push the where condition down into join filter
  // match 这个结构
  case f @ Filter(filterCondition, Join(left, right, joinType, joinCondition)) =>
    val (leftFilterConditions, rightFilterConditions, commonFilterCondition) =
      split(splitConjunctivePredicates(filterCondition), left, right)
    joinType match {
     // 是 inner join
      case _: InnerLike =>
        // left下推为Filter
        // push down the single side `where` condition into respective sides
        val newLeft = leftFilterConditions.
          reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
        // right下推为Filter
        val newRight = rightFilterConditions.
          reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
        val (newJoinConditions, others) =
          commonFilterCondition.partition(canEvaluateWithinJoin)
        val newJoinCond = (newJoinConditions ++ joinCondition).reduceLeftOption(And)
        // 最终的优化结果
        val join = Join(newLeft, newRight, joinType, newJoinCond)
        if (others.nonEmpty) {
          Filter(others.reduceLeft(And), join)
        } else {
          join
        }
```

Optimizer就介绍到这里，感兴趣大家可以多看看其他的优化规则，对sql肯定有更深刻的理解。