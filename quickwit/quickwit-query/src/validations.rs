use std::collections::HashSet;

use anyhow::{bail, Context};
use quickwit_proto::SearchRequest;
use tantivy::schema::{Field, FieldEntry, FieldType, Schema};

use crate::{SearchInputAst, SearchInputLeaf, SearchInputLiteral};

/// Extracts field names with ranges.
pub fn extract_field_with_ranges(
    schema: &Schema,
    search_input_ast: &SearchInputAst,
) -> anyhow::Result<Vec<String>> {
    let mut field_with_ranges = Vec::new();
    for leaf in collect_leaves(search_input_ast) {
        if let SearchInputLeaf::Range { field_opt, .. } = leaf {
            // TODO check the field supports ranges.
            if let Some(field_name) = field_opt {
                let field: Field = schema
                    .get_field(field_name.as_str())
                    .with_context(|| format!("Unknown field `{field_name}`"))?;
                let field_entry = schema.get_field_entry(field);
                is_valid_field_for_range(field_entry)?;
                field_with_ranges.push(field_name.clone());
            } else {
                anyhow::bail!("Range query without targeting a specific field is forbidden.");
            }
        }
    }
    Ok(field_with_ranges)
}

pub fn resolve_fields(schema: &Schema, field_names: &[String]) -> anyhow::Result<Vec<Field>> {
    let mut fields = vec![];
    for field_name in field_names {
        let field = schema.get_field(field_name)?;
        fields.push(field);
    }
    Ok(fields)
}

// Extract leaves from query ast.
fn collect_leaves(search_input_ast: &SearchInputAst) -> Vec<&SearchInputLeaf> {
    match search_input_ast {
        SearchInputAst::Clause(sub_queries) => {
            let mut leaves = vec![];
            for (_, sub_ast) in sub_queries {
                leaves.extend(collect_leaves(sub_ast));
            }
            leaves
        }
        SearchInputAst::Boost(ast, _) => collect_leaves(ast),
        SearchInputAst::Leaf(leaf) => vec![leaf],
    }
}

fn extract_field_name(leaf: &SearchInputLeaf) -> Option<&str> {
    match leaf {
        SearchInputLeaf::Literal(SearchInputLiteral { field_name_opt, .. }) => {
            field_name_opt.as_deref()
        }
        SearchInputLeaf::Range { field_opt, .. } => field_opt.as_deref(),
        SearchInputLeaf::Set { field_opt, .. } => field_opt.as_deref(),
        SearchInputLeaf::All => None,
    }
}

fn is_valid_field_for_range(field_entry: &FieldEntry) -> anyhow::Result<()> {
    match field_entry.field_type() {
        FieldType::Bool(_)
        | FieldType::Date(_)
        | FieldType::IpAddr(_)
        | FieldType::F64(_)
        | FieldType::I64(_)
        | FieldType::U64(_) => {
            if !field_entry.is_fast() {
                bail!(
                    "Range queries require having a fast field (field `{}` is not declared as a \
                     fast field.)",
                    field_entry.name()
                );
            }
        }
        other_type => {
            anyhow::bail!(
                "Field `{}` is of type `{:?}`. Range queries are only supported on boolean, \
                 datetime, IP, and numeric fields at the moment.",
                field_entry.name(),
                other_type.value_type()
            );
        }
    }
    Ok(())
}

pub fn extract_term_set_query_fields(search_input_ast: &SearchInputAst) -> Vec<String> {
    let mut field_set = Vec::new();
    field_set.extend(
        collect_leaves(search_input_ast)
            .into_iter()
            .filter_map(|leaf| {
                if let SearchInputLeaf::Set { field_opt, .. } = leaf {
                    field_opt.clone()
                } else {
                    None
                }
            }),
    );
    field_set.into_iter().collect()
}

/// Tells if the query has a Term or Range node which does not
/// specify a search field.
pub fn needs_default_search_field(user_input_ast: &SearchInputAst) -> bool {
    // `All` query apply to all fields, therefore doesn't need default fields.
    if matches!(user_input_ast, SearchInputAst::Leaf(leaf) if **leaf == SearchInputLeaf::All) {
        return false;
    }

    collect_leaves(user_input_ast)
        .into_iter()
        .any(|leaf| extract_field_name(leaf).is_none())
}

/// Collects all the fields names on the query ast nodes.
fn field_names(user_input_ast: &SearchInputAst) -> HashSet<&str> {
    collect_leaves(user_input_ast)
        .into_iter()
        .filter_map(extract_field_name)
        .collect()
}

pub fn validate_requested_snippet_fields(
    schema: &Schema,
    search_request: &SearchRequest,
    search_input_ast: &SearchInputAst,
    default_field_names: &[String],
) -> anyhow::Result<()> {
    let query_fields = field_names(search_input_ast);
    for field_name in &search_request.snippet_fields {
        if !default_field_names.contains(field_name)
            && !search_request.search_fields.contains(field_name)
            && !query_fields.contains(field_name.as_str())
        {
            return Err(anyhow::anyhow!(
                "The snippet field `{}` should be a default search field or appear in the query.",
                field_name
            ));
        }

        let field_entry = schema
            .get_field(field_name)
            .map(|field| schema.get_field_entry(field))?;
        match field_entry.field_type() {
            FieldType::Str(text_options) => {
                if !text_options.is_stored() {
                    return Err(anyhow::anyhow!(
                        "The snippet field `{}` must be stored.",
                        field_name
                    ));
                }
                continue;
            }
            other => {
                return Err(anyhow::anyhow!(
                    "The snippet field `{}` must be of type `Str`, got `{}`.",
                    field_name,
                    other.value_type().name()
                ))
            }
        }
    }
    Ok(())
}

pub fn validate_sort_by_field(
    field_name: &str,
    schema: &Schema,
    search_fields_opt: Option<&Vec<Field>>,
) -> anyhow::Result<()> {
    if field_name == "_score" {
        return validate_sort_by_score(schema, search_fields_opt);
    }
    let sort_by_field = schema
        .get_field(field_name)
        .with_context(|| format!("Unknown sort by field: `{field_name}`"))?;
    let sort_by_field_entry = schema.get_field_entry(sort_by_field);

    if matches!(sort_by_field_entry.field_type(), FieldType::Str(_)) {
        bail!(
            "Sort by field on type text is currently not supported `{}`.",
            field_name
        )
    }
    if !sort_by_field_entry.is_fast() {
        bail!(
            "Sort by field must be a fast field, please add the fast property to your field `{}`.",
            field_name
        )
    }

    Ok(())
}

fn validate_sort_by_score(
    schema: &Schema,
    search_fields_opt: Option<&Vec<Field>>,
) -> anyhow::Result<()> {
    if let Some(fields) = search_fields_opt {
        for field in fields {
            if !schema.get_field_entry(*field).has_fieldnorms() {
                bail!(
                    "Fieldnorms for field `{}` is missing. Fieldnorms must be stored for the \
                     field to compute the BM25 score of the documents.",
                    schema.get_field_name(*field)
                )
            }
        }
    }
    Ok(())
}
