//! Strict SQL subset parser (review P1 #3).
//!
//! Accepts:
//!   `SELECT [group_cols,...] [SUM(quantity)] [COUNT(*)]
//!    FROM (usage_events | usage_rollup_hourly)
//!    [WHERE conjunction-of-equalities-and-ranges]
//!    [GROUP BY col, col, ...]
//!    [LIMIT N]`
//!
//! Rejects anything else with a precise error. The previous permissive
//! parser silently mapped `SUM(any_column) → SUM(quantity)`, treated
//! `<`/`<=` and `>`/`>=` interchangeably, dropped `OR`/`HAVING`/aliases,
//! and accepted unsupported AST nodes — all wrong-answer territory for
//! billing queries.

use sqlparser::parser::Parser;
use sqlparser::dialect::GenericDialect;
use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, OrderByKind, SelectItem, SetExpr, Statement, TableFactor,
    Value as SqlValue,
};
use crate::query::plan::{AggregationFunction, QueryFilter, QueryPlan, QuerySource};
use std::collections::HashMap;

pub fn parse_sql(sql: &str) -> Result<QueryPlan, String> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;

    if ast.len() != 1 {
        return Err(format!("expected exactly one statement, got {}", ast.len()));
    }
    let Statement::Query(query) = &ast[0] else {
        return Err("only SELECT statements are supported".into());
    };

    // ORDER BY / OFFSET / WITH are not supported.
    if let Some(order) = &query.order_by {
        let has_exprs = match &order.kind {
            OrderByKind::Expressions(exprs) => !exprs.is_empty(),
            OrderByKind::All(_) => true,
        };
        if has_exprs {
            return Err("ORDER BY is not supported".into());
        }
    }
    if query.with.is_some() {
        return Err("WITH/CTE is not supported".into());
    }

    let SetExpr::Select(select) = &*query.body else {
        return Err("compound queries (UNION/INTERSECT/EXCEPT) are not supported".into());
    };
    if select.distinct.is_some() {
        return Err("SELECT DISTINCT is not supported".into());
    }
    if !select.named_window.is_empty() {
        return Err("WINDOW clauses are not supported".into());
    }
    if select.having.is_some() {
        return Err("HAVING is not supported".into());
    }
    if select.qualify.is_some() {
        return Err("QUALIFY is not supported".into());
    }
    if !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
    {
        return Err("CLUSTER BY / DISTRIBUTE BY / SORT BY are not supported".into());
    }

    if select.from.len() != 1 {
        return Err("exactly one FROM table is required".into());
    }
    let from = &select.from[0];
    if !from.joins.is_empty() {
        return Err("JOINs are not supported".into());
    }
    let TableFactor::Table { name, alias, args, version, .. } = &from.relation else {
        return Err("FROM must reference a simple table".into());
    };
    if alias.is_some() {
        return Err("table aliases are not supported".into());
    }
    if args.is_some() {
        return Err("table-valued function syntax is not supported".into());
    }
    if version.is_some() {
        return Err("AS OF / table version qualifiers are not supported".into());
    }

    let source_name = name.to_string();
    let source = match source_name.as_str() {
        "usage_events" => QuerySource::RawEvents,
        "usage_rollup_hourly" => QuerySource::RollupHourly,
        _ => return Err(format!("unknown table `{}` (expected usage_events or usage_rollup_hourly)", source_name)),
    };

    // Projection: each item is either a bare identifier (a group column)
    // or one of the supported aggregates (SUM(quantity) / COUNT(*)).
    let mut group_by_in_select: Vec<String> = Vec::new();
    let mut metrics: HashMap<String, AggregationFunction> = HashMap::new();

    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => match expr {
                Expr::Identifier(ident) => {
                    group_by_in_select.push(ident.value.clone());
                }
                Expr::CompoundIdentifier(parts) if parts.len() == 1 => {
                    group_by_in_select.push(parts[0].value.clone());
                }
                Expr::Function(func) => {
                    let (name, metric) = parse_aggregate(func)?;
                    metrics.insert(name, metric);
                }
                _ => return Err(format!("unsupported projection expression: {}", expr)),
            },
            SelectItem::ExprWithAlias { .. } => {
                return Err("column aliases (AS) are not supported".into());
            }
            SelectItem::Wildcard(_) => {
                return Err("SELECT * is not supported — specify columns or aggregates".into());
            }
            SelectItem::QualifiedWildcard(..) => {
                return Err("qualified wildcards (table.*) are not supported".into());
            }
            SelectItem::ExprWithAliases { .. } => {
                return Err("multi-alias projections are not supported".into());
            }
        }
    }

    // GROUP BY clause — combined with any group columns from the projection.
    let mut group_by = group_by_in_select;
    match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) => {
            if !modifiers.is_empty() {
                return Err("GROUP BY modifiers (ROLLUP/CUBE/etc) are not supported".into());
            }
            for expr in exprs {
                match expr {
                    Expr::Identifier(ident) => {
                        if !group_by.iter().any(|g| g == &ident.value) {
                            group_by.push(ident.value.clone());
                        }
                    }
                    _ => return Err(format!("unsupported GROUP BY expression: {}", expr)),
                }
            }
        }
        GroupByExpr::All(_) => {
            return Err("GROUP BY ALL is not supported".into());
        }
    }

    let mut plan = QueryPlan {
        source,
        account_id: None,
        from_ms: i64::MIN,
        to_ms: i64::MAX,
        filters: Vec::new(),
        group_by,
        metrics,
        limit: None,
    };

    if let Some(selection) = &select.selection {
        extract_filters(selection, &mut plan)?;
    }

    // LIMIT — accept integer literal or none.
    if let Some(limit_expr) = &query.limit_clause {
        return Err(format!(
            "LIMIT is not supported in this build (got `{}`)",
            limit_expr
        ));
    }

    Ok(plan)
}

fn parse_aggregate(
    func: &sqlparser::ast::Function,
) -> Result<(String, AggregationFunction), String> {
    let name = func.name.to_string().to_uppercase();
    let args_str = match &func.args {
        sqlparser::ast::FunctionArguments::List(list) => list,
        _ => return Err(format!("unsupported function arguments for {}", name)),
    };
    if !args_str.duplicate_treatment.is_none() {
        return Err(format!("{}: DISTINCT/ALL not supported", name));
    }
    if !args_str.clauses.is_empty() {
        return Err(format!("{}: extra argument clauses not supported", name));
    }

    let args = &args_str.args;
    match name.as_str() {
        "SUM" => {
            if args.len() != 1 {
                return Err(format!("SUM takes exactly one argument, got {}", args.len()));
            }
            let arg_expr = function_arg_expr(&args[0])?;
            match arg_expr {
                Expr::Identifier(ident) if ident.value == "quantity" => {
                    Ok(("quantity".to_string(), AggregationFunction::Sum))
                }
                _ => Err(format!(
                    "SUM only supports the `quantity` column; got SUM({})",
                    arg_expr
                )),
            }
        }
        "COUNT" => {
            if args.len() != 1 {
                return Err(format!("COUNT takes exactly one argument, got {}", args.len()));
            }
            match &args[0] {
                sqlparser::ast::FunctionArg::Unnamed(
                    sqlparser::ast::FunctionArgExpr::Wildcard,
                ) => Ok(("count".to_string(), AggregationFunction::Count)),
                _ => Err(
                    "COUNT only supports COUNT(*) in this build — column-counting not implemented"
                        .into(),
                ),
            }
        }
        other => Err(format!("unsupported aggregate function: {}", other)),
    }
}

fn function_arg_expr(arg: &sqlparser::ast::FunctionArg) -> Result<&Expr, String> {
    match arg {
        sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => Ok(e),
        sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Wildcard) => {
            Err("wildcard not valid here".into())
        }
        sqlparser::ast::FunctionArg::Unnamed(
            sqlparser::ast::FunctionArgExpr::QualifiedWildcard(_),
        ) => Err("qualified wildcard not valid here".into()),
        sqlparser::ast::FunctionArg::Unnamed(
            sqlparser::ast::FunctionArgExpr::WildcardWithOptions(_),
        ) => Err("wildcard with options not valid here".into()),
        sqlparser::ast::FunctionArg::Named { .. } => {
            Err("named function arguments are not supported".into())
        }
        sqlparser::ast::FunctionArg::ExprNamed { .. } => {
            Err("named expression arguments are not supported".into())
        }
    }
}

/// Walk a WHERE clause and pull out filters / time range. AND-only; reject
/// OR explicitly so it doesn't silently match nothing.
fn extract_filters(expr: &Expr, plan: &mut QueryPlan) -> Result<(), String> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                extract_filters(left, plan)?;
                extract_filters(right, plan)?;
                Ok(())
            }
            BinaryOperator::Or => Err("OR in WHERE is not supported".into()),
            BinaryOperator::Eq => apply_eq(left, right, plan),
            BinaryOperator::GtEq | BinaryOperator::Gt | BinaryOperator::Lt | BinaryOperator::LtEq => {
                apply_range(left, op, right, plan)
            }
            other => Err(format!("unsupported operator `{:?}` in WHERE", other)),
        },
        Expr::InList { expr: left, list, negated: false } => {
            let field = match &**left {
                Expr::Identifier(i) => i.value.clone(),
                _ => return Err("IN: left side must be a column name".into()),
            };
            let mut values = Vec::with_capacity(list.len());
            for v in list {
                values.push(extract_literal(v)?);
            }
            apply_field(field, values, plan);
            Ok(())
        }
        Expr::InList { negated: true, .. } => Err("NOT IN is not supported".into()),
        _ => Err(format!("unsupported WHERE expression: {}", expr)),
    }
}

fn apply_eq(left: &Expr, right: &Expr, plan: &mut QueryPlan) -> Result<(), String> {
    let field = match left {
        Expr::Identifier(i) => i.value.clone(),
        _ => return Err("equality: left side must be a column name".into()),
    };
    let value = extract_literal(right)?;
    apply_field(field, vec![value], plan);
    Ok(())
}

fn apply_field(field: String, values: Vec<String>, plan: &mut QueryPlan) {
    if field == "account_id" && values.len() == 1 {
        plan.account_id = Some(values.into_iter().next().unwrap());
    } else {
        plan.filters.push(QueryFilter { field, values });
    }
}

fn apply_range(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    plan: &mut QueryPlan,
) -> Result<(), String> {
    let field = match left {
        Expr::Identifier(i) => i.value.clone(),
        _ => return Err("range comparison: left side must be a column name".into()),
    };
    if field != "timestamp_ms" && field != "hour_start_ms" {
        return Err(format!(
            "range comparison only supported on timestamp_ms / hour_start_ms (got `{}`)",
            field
        ));
    }
    let v: i64 = extract_literal(right)?
        .parse()
        .map_err(|e| format!("range comparison value must parse as i64: {}", e))?;

    // Convert to half-open [from, to). Inclusive bounds become exclusive
    // by adjusting by ±1.
    match op {
        BinaryOperator::Gt => plan.from_ms = plan.from_ms.max(v.saturating_add(1)),
        BinaryOperator::GtEq => plan.from_ms = plan.from_ms.max(v),
        BinaryOperator::Lt => plan.to_ms = plan.to_ms.min(v),
        BinaryOperator::LtEq => plan.to_ms = plan.to_ms.min(v.saturating_add(1)),
        _ => unreachable!("apply_range called with non-comparison op"),
    }
    Ok(())
}

fn extract_literal(val: &Expr) -> Result<String, String> {
    match val {
        Expr::Value(v) => match &v.value {
            SqlValue::SingleQuotedString(s) => Ok(s.clone()),
            SqlValue::Number(n, _) => Ok(n.clone()),
            other => Err(format!("unsupported literal: {:?}", other)),
        },
        _ => Err(format!("expected a literal, got `{}`", val)),
    }
}
