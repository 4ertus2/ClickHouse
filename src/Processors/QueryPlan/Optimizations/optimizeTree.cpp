#include <Common/Exception.h>
#include <Processors/QueryPlan/ReadFromLocalReplica.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/UnionStep.h>

#include <stack>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int TOO_MANY_QUERY_PLAN_OPTIMIZATIONS;
    extern const int PROJECTION_NOT_USED;
}

namespace QueryPlanOptimizations
{

void optimizeTreeFirstPass(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root, QueryPlan::Nodes & nodes)
{
    if (!optimization_settings.optimize_plan)
        return;

    const auto & optimizations = getOptimizations();

    struct Frame
    {
        QueryPlan::Node * node = nullptr;

        /// If not zero, traverse only depth_limit layers of tree (if no other optimizations happen).
        /// Otherwise, traverse all children.
        size_t depth_limit = 0;

        /// Next child to process.
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    stack.push({.node = &root});

    const size_t max_optimizations_to_apply = optimization_settings.max_optimizations_to_apply;
    size_t total_applied_optimizations = 0;


    Optimization::ExtraSettings extra_settings = {
        optimization_settings.max_limit_for_vector_search_queries,
        optimization_settings.vector_search_filter_strategy,
        optimization_settings.use_index_for_in_with_subqueries_max_values,
        optimization_settings.network_transfer_limits,
    };

    while (!stack.empty())
    {
        auto & frame = stack.top();

        /// If traverse_depth_limit == 0, then traverse without limit (first entrance)
        /// If traverse_depth_limit > 1, then traverse with (limit - 1)
        if (frame.depth_limit != 1)
        {
            /// Traverse all children first.
            if (frame.next_child < frame.node->children.size())
            {
                stack.push(
                {
                    .node = frame.node->children[frame.next_child],
                    .depth_limit = frame.depth_limit ? (frame.depth_limit - 1) : 0,
                });

                ++frame.next_child;
                continue;
            }
        }

        size_t max_update_depth = 0;

        /// Apply all optimizations.
        for (const auto & optimization : optimizations)
        {
            if (!(optimization_settings.*(optimization.is_enabled)))
                continue;

            /// Just in case, skip optimization if it is not initialized.
            if (!optimization.apply)
                continue;

            if (max_optimizations_to_apply && max_optimizations_to_apply < total_applied_optimizations)
            {
                if (optimization_settings.is_explain)
                    return;

                throw Exception(ErrorCodes::TOO_MANY_QUERY_PLAN_OPTIMIZATIONS,
                                "Too many optimizations applied to query plan. Current limit {}",
                                max_optimizations_to_apply);
            }


            /// Try to apply optimization.
            auto update_depth = optimization.apply(frame.node, nodes, extra_settings);
            if (update_depth)
                ++total_applied_optimizations;
            max_update_depth = std::max<size_t>(max_update_depth, update_depth);
        }

        /// Traverse `max_update_depth` layers of tree again.
        if (max_update_depth)
        {
            frame.depth_limit = max_update_depth;
            frame.next_child = 0;
            continue;
        }

        /// Nothing was applied.
        stack.pop();
    }
}

void optimizeTreeSecondPass(
    const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root, QueryPlan::Nodes & nodes, QueryPlan & query_plan)
{
    const size_t max_optimizations_to_apply = optimization_settings.max_optimizations_to_apply;
    std::unordered_set<String> applied_projection_names;
    bool has_reading_from_mt = false;

    Stack stack;
    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        optimizePrimaryKeyConditionAndLimit(stack);

        updateQueryConditionCache(stack, optimization_settings);

        /// NOTE: optimizePrewhere can modify the stack.
        /// Prewhere optimization relies on PK optimization (getConditionSelectivityEstimatorByPredicate)
        if (optimization_settings.optimize_prewhere)
            optimizePrewhere(stack, nodes);

        auto & frame = stack.back();

        /// Traverse all children first.
        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }

        stack.pop_back();
    }

    calculateHashTableCacheKeys(root);

    stack.push_back({.node = &root});
    while (!stack.empty())
    {
        auto & frame = stack.back();

        if (frame.next_child == 0)
        {
            const auto rhs_estimation = optimizeJoinLogical(*frame.node, nodes, optimization_settings);
            bool has_join_logical = convertLogicalJoinToPhysical(*frame.node, nodes, optimization_settings, rhs_estimation);
            if (!has_join_logical)
                optimizeJoinLegacy(*frame.node, nodes, optimization_settings);

            if (optimization_settings.read_in_order)
                optimizeReadInOrder(*frame.node, nodes);

            if (optimization_settings.distinct_in_order)
                optimizeDistinctInOrder(*frame.node, nodes);
        }

        /// Traverse all children first.
        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }

        stack.pop_back();
    }

    // find ReadFromLocalParallelReplicaStep and replace with optimized local plan
    bool read_from_local_parallel_replica_plan = false;
    stack.push_back({.node = &root});
    while (!stack.empty())
    {
        auto & frame = stack.back();

        /// Traverse all children first.
        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }
        if (auto * read_from_local = typeid_cast<ReadFromLocalParallelReplicaStep*>(frame.node->step.get()))
        {
            read_from_local_parallel_replica_plan = true;

            auto local_plan = read_from_local->extractQueryPlan();
            local_plan->optimize(optimization_settings);

            auto * local_plan_node = frame.node;
            query_plan.replaceNodeWithPlan(local_plan_node, std::move(local_plan));

            // after applying optimize() we still can have several expression in a row,
            // so merge them to make plan more concise
            if (optimization_settings.merge_expressions)
                tryMergeExpressions(local_plan_node, nodes, {});
        }

        stack.pop_back();
    }
    // local plan can contain redundant sorting
    if (read_from_local_parallel_replica_plan && optimization_settings.remove_redundant_sorting)
        tryRemoveRedundantSorting(&root);


    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        {
            /// NOTE: frame cannot be safely used after stack was modified.
            auto & frame = stack.back();

            if (frame.next_child == 0)
            {
                has_reading_from_mt |= typeid_cast<const ReadFromMergeTree *>(frame.node->step.get()) != nullptr;

                /// Projection optimization relies on PK optimization
                if (optimization_settings.optimize_projection)
                {
                    auto applied_projection = optimizeUseAggregateProjections(*frame.node, nodes, optimization_settings.optimize_use_implicit_projections);
                    if (applied_projection)
                        applied_projection_names.insert(*applied_projection);
                }

                if (optimization_settings.aggregation_in_order)
                    optimizeAggregationInOrder(*frame.node, nodes);
            }

            /// Traverse all children first.
            if (frame.next_child < frame.node->children.size())
            {
                auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
                ++frame.next_child;
                stack.push_back(next_frame);
                continue;
            }
        }

        if (optimization_settings.optimize_projection)
        {
            /// Projection optimization relies on PK optimization
            if (auto applied_projection = optimizeUseNormalProjections(stack, nodes))
            {
                applied_projection_names.insert(*applied_projection);

                if (max_optimizations_to_apply && max_optimizations_to_apply < applied_projection_names.size())
                {
                    /// Limit only first pass in EXPLAIN mode.
                    if (!optimization_settings.is_explain)
                        throw Exception(ErrorCodes::TOO_MANY_QUERY_PLAN_OPTIMIZATIONS,
                                        "Too many projection optimizations applied to query plan. Current limit {}",
                                        max_optimizations_to_apply);
                }

                /// Stack is updated after this optimization and frame is not valid anymore.
                /// Try to apply optimizations again to newly added plan steps.
                --stack.back().next_child;
                continue;
            }
        }

        stack.pop_back();
    }

    /// projection optimizations can introduce additional reading step
    /// so, applying lazy materialization after it, since it's dependent on reading step
    if (optimization_settings.optimize_lazy_materialization)
    {
        chassert(stack.empty());
        stack.push_back({.node = &root});
        while (!stack.empty())
        {
            auto & frame = stack.back();

            if (frame.next_child == 0)
            {
                if (optimizeLazyMaterialization(root, stack, nodes, optimization_settings.max_limit_for_lazy_materialization))
                    break;
            }

            /// Traverse all children first.
            if (frame.next_child < frame.node->children.size())
            {
                auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
                ++frame.next_child;
                stack.push_back(next_frame);
                continue;
            }

            stack.pop_back();
        }
    }

    if (optimization_settings.force_use_projection && has_reading_from_mt && applied_projection_names.empty())
        throw Exception(
            ErrorCodes::PROJECTION_NOT_USED,
            "No projection is used when optimize_use_projections = 1 and force_optimize_projection = 1");

    if (!optimization_settings.force_projection_name.empty() && has_reading_from_mt && !applied_projection_names.contains(optimization_settings.force_projection_name))
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Projection {} is specified in setting force_optimize_projection_name but not used",
             optimization_settings.force_projection_name);

    /// Trying to reuse sorting property for other steps.
    applyOrder(optimization_settings, root);

    if (optimization_settings.query_plan_join_shard_by_pk_ranges)
        optimizeJoinByShards(root);
}

void addStepsToBuildSets(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan & plan, QueryPlan::Node & root, QueryPlan::Nodes & nodes)
{
    Stack stack;
    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        /// NOTE: frame cannot be safely used after stack was modified.
        auto & frame = stack.back();

        /// Traverse all children first.
        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }

        addPlansForSets(optimization_settings, plan, *frame.node, nodes);

        stack.pop_back();
    }
}

}
}
