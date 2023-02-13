extern crate proc_macro;
use std::path::{Path, PathBuf};

use proc_macro::TokenStream;
use quote::{format_ident, quote, ToTokens};
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, Error, LitStr, Token, Type,
};
use trustfall_core::{
    frontend::parse,
    interpreter::{
        query_plan::{query_plan, CoerceKind, QueryPlan, QueryPlanItem},
        FieldRef,
    },
    ir::{Eid, FoldSpecificFieldKind, Vid},
    schema::Schema,
};

#[proc_macro]
pub fn compile_query(tokens: TokenStream) -> TokenStream {
    let input = parse_macro_input!(tokens as Input);

    match input.process() {
        Ok(v) => v.to_token_stream(),
        Err(err) => err.to_compile_error(),
    }
    .into()
}

struct Input {
    typ: Type,
    _comma1: Token![,],
    schema_path: LitStr,
    _comma2: Token![,],
    query: LitStr,
}

impl Parse for Input {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Ok(Input {
            typ: input.parse()?,
            _comma1: input.parse()?,
            schema_path: input.parse()?,
            _comma2: input.parse()?,
            query: input.parse()?,
        })
    }
}

impl Input {
    fn process(self) -> Result<Output, Error> {
        let schema_path = self.schema_path.value();
        let path = Path::new(&schema_path).canonicalize().map_err(|_| {
            Error::new(
                self.schema_path.span(),
                format!("failed to find schema file {schema_path:?}"),
            )
        })?;
        let schema_src = std::fs::read_to_string(&path)
            .map_err(|_| Error::new(self.schema_path.span(), format!("failed to read {path:?}")))?;

        let schema = Schema::parse(schema_src).unwrap();
        let query = parse(&schema, self.query.value().as_str())
            .map_err(|e| Error::new(self.query.span(), e))?;

        let plan = query_plan(query);

        Ok(Output {
            plan,
            path,
            typ: self.typ,
        })
    }
}

struct Output {
    plan: QueryPlan,
    path: PathBuf,
    typ: Type,
}

impl ToTokens for Output {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let Self {
            plan:
                QueryPlan {
                    params,
                    edge,
                    vid,
                    plan,
                    outputs,
                    variables,
                },
            path,
            typ,
        } = self;

        let mut block = proc_macro2::TokenStream::new();

        let path = path.to_string_lossy();
        block.extend(quote!(
            // needed for rerun-if-changed
            const _: () = {
                let _ = include_str!(#path);
            };
        ));

        // get_starting_tokens
        let edge_name = format_ident!("{edge}");
        let vid = VidToken(*vid);
        block.extend(quote!(
            let iter = <#typ>::#edge_name(self, #vid);
        ));

        BuildPlan {
            typ: &typ,
            plan: plan.as_slice(),
        }
        .to_tokens(&mut block);

        tokens.extend(quote!(
            struct Arguments;
            struct Output;

            impl #typ {
                fn query(&self, arguments: Arguments) -> impl std::iter::Iterator<Item = Output> {
                    #block
                }
            }
        ))

        // outputs
    }
}

struct BuildPlan<'a> {
    typ: &'a Type,
    plan: &'a [QueryPlanItem],
}

impl ToTokens for BuildPlan<'_> {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let Self { typ, plan } = self;
        for item in plan.iter() {
            match item {
                QueryPlanItem::ProjectProperty {
                    type_name,
                    vid,
                    field_name,
                } => {
                    let field_name = format_ident!("{field_name}");
                    // let type_name = format_ident!("{type_name}");
                    // let field_name = field_name.as_ref();
                    let type_name = type_name.as_ref();
                    let vid = VidToken(*vid);
                    tokens.extend(quote!(
                        let iter = <#typ>::#field_name(self, iter, #type_name, #vid);
                        let iter = ::trustfall_core::interpreter::executer_components::save_property_value(iter);
                    ))
                }
                QueryPlanItem::Coerce {
                    coerced_from,
                    coerce_to,
                    vid,
                    kind,
                } => {
                    // let coerced_from = format_ident!("{coerced_from}");
                    // let coerce_to = format_ident!("{coerce_to}");
                    let coerced_from = coerced_from.as_ref();
                    let coerce_to = coerce_to.as_ref();
                    let vid = VidToken(*vid);
                    tokens.extend(quote!(
                        let iter = <#typ>::coerce(self, iter, #coerced_from, #coerce_to, #vid);
                    ));

                    match kind {
                        CoerceKind::Filter => {
                            tokens.extend(quote!(
                                let iter = ::trustfall_core::interpreter::executer_components::coerce_filter(iter);
                            ));
                        }
                        CoerceKind::Suspend => {
                            tokens.extend(quote!(
                                let iter = ::trustfall_core::interpreter::executer_components::coerce_suspend(iter);
                            ));
                        }
                        _ => unimplemented!(),
                    }
                }
                QueryPlanItem::Neighbors {
                    type_name,
                    edge_name,
                    parameters,
                    eid,
                    vid,
                    kind,
                } => {
                    let edge_name = format_ident!("{edge_name}");
                    let type_name = type_name.as_ref();
                    let eid = EidToken(*eid);
                    let vid = VidToken(*vid);

                    tokens.extend(quote!(
                        let neighbor_iter = <#typ>::#edge_name(self, iter, #type_name, #eid, #vid);
                    ));

                    match kind {
                        trustfall_core::interpreter::query_plan::ExpandKind::Required => {
                            tokens.extend(quote!(
                                let iter = ::trustfall_core::interpreter::executer_components::expand_neighbors_required(neighbor_iter);
                            ))
                        },
                        trustfall_core::interpreter::query_plan::ExpandKind::Optional => {
                            tokens.extend(quote!(
                                let iter = ::trustfall_core::interpreter::executer_components::expand_neighbors_optional(neighbor_iter);
                            ))
                        },
                        trustfall_core::interpreter::query_plan::ExpandKind::Recursive => {
                            tokens.extend(quote!(
                                let iter = ::trustfall_core::interpreter::executer_components::expand_neighbors_recurse(neighbor_iter);
                            ))
                        },
                        trustfall_core::interpreter::query_plan::ExpandKind::Fold { plan, tags, post_fold_filters } => {},
                        _ => unimplemented!(),
                    }
                }
                QueryPlanItem::ImportTag(field) => {
                    let field_ref = FieldRefToken(field.clone());
                    tokens.extend(quote!(
                        let field_ref = #field_ref;
                        let iter = ::trustfall_core::interpreter::executer_components::import_tag(iter, field_ref);
                    ));
                }
                QueryPlanItem::PopIntoImport(field) =>{
                    let field_ref = FieldRefToken(field.clone());
                    tokens.extend(quote!(
                        let field_ref = #field_ref;
                        let iter = ::trustfall_core::interpreter::executer_components::pop_into_import(iter, field_ref);
                    ));
                }
                QueryPlanItem::Argument(field) => {
                    let ident = format_ident!("{field}");
                    tokens.extend(quote!(
                        let value = arguments.#ident.clone();
                        let iter = ::trustfall_core::interpreter::executer_components::push_value(iter, value);
                    ))
                }
                QueryPlanItem::FoldCount(eid) => {
                    let eid = EidToken(*eid);
                    tokens.extend(quote!(
                        let iter = ::trustfall_core::interpreter::executer_components::fold_count(iter, #eid);
                    ))
                }
                QueryPlanItem::Filter(_) => {}
                QueryPlanItem::Record(vid) => {
                    let vid = VidToken(*vid);
                    tokens.extend(quote!(
                        let iter = ::trustfall_core::interpreter::executer_components::record(iter, #vid);
                    ))
                }
                QueryPlanItem::Activate(vid) => {
                    let vid = VidToken(*vid);
                    tokens.extend(quote!(
                        let iter = ::trustfall_core::interpreter::executer_components::activate(iter, #vid);
                    ))
                }
                QueryPlanItem::Unsuspend => {
                    tokens.extend(quote!(
                        let iter = ::trustfall_core::interpreter::executer_components::unsuspend(iter);
                    ))
                }
                QueryPlanItem::SuspendNone => {
                    tokens.extend(quote!(
                        let iter = ::trustfall_core::interpreter::executer_components::suspend_none(iter);
                    ))
                }
                QueryPlanItem::Suspend => {
                    tokens.extend(quote!(
                        let iter = ::trustfall_core::interpreter::executer_components::suspend(iter);
                    ))
                }
                QueryPlanItem::FoldOutputs {
                    eid,
                    vid,
                    output_names,
                    fold_specific_outputs,
                    folded_keys,
                    plan,
                } => {}
                QueryPlanItem::RecursePostProcess => {
                    tokens.extend(quote!(
                        let iter = ::trustfall_core::interpreter::executer_components::post_process_recursive_expansion(iter);
                    ))
                }
                _ => unimplemented!()
            }
        }
    }
}

struct VidToken(Vid);
impl ToTokens for VidToken {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let n = self.0.get().get();
        tokens.extend(
            quote!(::trustfall_core::ir::Vid::new(::std::num::NonZeroUsize::new(#n).unwrap()) ),
        );
    }
}

struct EidToken(Eid);
impl ToTokens for EidToken {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let n = self.0.get().get();
        tokens.extend(
            quote!(::trustfall_core::ir::Eid::new(::std::num::NonZeroUsize::new(#n).unwrap()) ),
        );
    }
}

struct FieldRefToken(FieldRef);

impl ToTokens for FieldRefToken {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        match &self.0 {
            FieldRef::ContextField(c) => {
                let vid = VidToken(c.vertex_id);
                let field_name = c.field_name.as_ref();
                tokens.extend(quote!(
                    ::trustfall_core::interpreter::FieldRef::ContextField(
                        ::trustfall_core::interpreter::ContextField {
                            vertex_id: #vid,
                            field_name: ::std::sync::Arc::new(#field_name),
                        },
                    )
                ));
            }
            FieldRef::FoldSpecificField(fsf) => {
                let eid = EidToken(fsf.fold_eid);
                let vid = VidToken(fsf.fold_root_vid);
                match fsf.kind {
                    FoldSpecificFieldKind::Count => tokens.extend(quote!(
                        ::trustfall_core::interpreter::FieldRef::FoldSpecificField(
                            ::trustfall_core::ir::FoldSpecificField {
                                fold_eid: #eid,
                                fold_root_vid: #vid,
                                kind: ::trustfall_core::ir::FoldSpecificFieldKind::Count,
                            },
                        )
                    )),
                    _ => unimplemented!(),
                }
            }
        }
    }
}
