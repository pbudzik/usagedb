use sqlparser::parser::Parser;
use sqlparser::dialect::GenericDialect;
use sqlparser::ast::{Statement, SetExpr, Expr, SelectItem, TableFactor, BinaryOperator};
use crate::query::plan::{QueryPlan, QuerySource, QueryFilter, AggregationFunction};
use std::collections::HashMap;

pub fn parse_sql(sql: &str) -> Result<QueryPlan, String> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;
    
    if ast.is_empty() {
        return Err("Empty query".into());
    }

    let Statement::Query(query) = &ast[0] else {
        return Err("Only SELECT queries are supported".into());
    };

    let SetExpr::Select(select) = &*query.body else {
        return Err("Only simple SELECT queries are supported".into());
    };

    if select.from.is_empty() {
        return Err("FROM clause is missing".into());
    }
    
    let TableFactor::Table { name, .. } = &select.from[0].relation else {
        return Err("Unsupported FROM clause".into());
    };
    
    let source_name = name.to_string();
    let source = match source_name.as_str() {
        "usage_events" => QuerySource::RawEvents,
        "usage_rollup_hourly" => QuerySource::RollupHourly,
        _ => return Err(format!("Unknown table: {}", source_name)),
    };

    let mut plan = QueryPlan {
        source,
        account_id: None,
        from_ms: 0,
        to_ms: i64::MAX,
        filters: Vec::new(),
        group_by: Vec::new(),
        metrics: HashMap::new(),
        limit: None,
    };

    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                match expr {
                    Expr::Function(func) => {
                        let name = func.name.to_string().to_uppercase();
                        if name == "SUM" {
                            plan.metrics.insert("quantity".to_string(), AggregationFunction::Sum);
                        } else if name == "COUNT" {
                            plan.metrics.insert("count".to_string(), AggregationFunction::Count);
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    if let Some(selection) = &select.selection {
        extract_filters(selection, &mut plan)?;
    }

    match &select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) => {
            for expr in exprs {
                if let Expr::Identifier(ident) = expr {
                    plan.group_by.push(ident.value.clone());
                }
            }
        }
        _ => {}
    }

    // Limit processing removed for MVP

    Ok(plan)
}

fn extract_filters(expr: &Expr, plan: &mut QueryPlan) -> Result<(), String> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if *op == BinaryOperator::And {
                extract_filters(left, plan)?;
                extract_filters(right, plan)?;
            } else if *op == BinaryOperator::Eq {
                if let Expr::Identifier(ident) = &**left {
                    let field = ident.value.clone();
                    let value = extract_value(right);
                    if field == "account_id" {
                        plan.account_id = Some(value.clone());
                    } else {
                        plan.filters.push(QueryFilter { field, values: vec![value] });
                    }
                }
            } else if *op == BinaryOperator::GtEq || *op == BinaryOperator::Gt {
                if let Expr::Identifier(ident) = &**left {
                    if ident.value == "hour_start_ms" || ident.value == "timestamp_ms" {
                        plan.from_ms = extract_value(right).parse().unwrap_or(0);
                    }
                }
            } else if *op == BinaryOperator::LtEq || *op == BinaryOperator::Lt {
                if let Expr::Identifier(ident) = &**left {
                    if ident.value == "hour_start_ms" || ident.value == "timestamp_ms" {
                        plan.to_ms = extract_value(right).parse().unwrap_or(i64::MAX);
                    }
                }
            }
        }
        Expr::InList { expr: left, list, negated: false } => {
            if let Expr::Identifier(ident) = &**left {
                let mut values = Vec::new();
                for val in list {
                    values.push(extract_value(val));
                }
                plan.filters.push(QueryFilter { field: ident.value.clone(), values });
            }
        }
        _ => {}
    }
    Ok(())
}

fn extract_value(val: &sqlparser::ast::Expr) -> String {
    if let Expr::Value(v) = val {
        match &v.value {
            sqlparser::ast::Value::SingleQuotedString(s) => return s.clone(),
            sqlparser::ast::Value::Number(n, _) => return n.clone(),
            _ => {}
        }
    }
    "".to_string()
}
