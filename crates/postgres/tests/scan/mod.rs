//! The statement scan's parser (TE §1.3 "PostgreSQL"): it reads a Rust
//! source with `syn` and reports every place that may hand SQL to the
//! driver instead of a registered `Statement`. Comments and string
//! contents never match, since they are not code.
//!
//! What counts as a driver call:
//!
//! - a method call `receiver.m(…)`, turbofish or not, of a
//!   [`DRIVER_METHODS`] name with exactly the driver's argument count,
//!   unless its second argument is `&A_STATEMENT`, a constant the scanned
//!   sources declare with the type `Statement` and the same file does not
//!   declare as another constant or static (the registry's methods take
//!   the session first and the statement second; a parameter list such as
//!   `&NONE` is not one);
//! - a path call `T::m(…)` or `<T as Trait>::m(…)` and a method reference
//!   `T::m` of a [`DRIVER_METHODS`] name whose path names a
//!   [`DRIVER_TYPES`] type, a type of a [`DRIVER_CRATES`] crate, or a name
//!   the same file binds to one of them (`use … as G`, `type C = …`, a
//!   generic parameter bounded by one, in a where clause too);
//! - inside a `macro_rules!` body, the same calls found on its tokens, a
//!   path through a metavariable (`$t::execute(…)`, `<$t>::execute(…)`),
//!   and any method named by a metavariable (`$client.$m(…)`,
//!   `<$t>::$m(…)`), which the scan cannot resolve;
//! - inside any other macro invocation, the same rules on its arguments
//!   when they parse as expressions or statements, otherwise on its tokens.
//!
//! The scan has no types: `T::m` through an alias another file declares,
//! or a driver value reached through a generic function, stays invisible.
use proc_macro2::{Delimiter, Spacing, TokenStream, TokenTree};
use std::collections::BTreeSet;
use syn::{
    Expr, ExprCall, ExprMethodCall, ExprPath, Macro, Path, Token, Type,
    punctuated::Punctuated,
    visit::{self, Visit},
};

/// Driver methods that take SQL text, a driver statement or a portal
/// first, with their argument count: every such public method of
/// `tokio_postgres`' `Client`, `Transaction` and `GenericClient` and of
/// `deadpool_postgres`' `ClientWrapper`, `Transaction` and
/// `GenericClient`, which `statement_scan.rs` checks against the locked
/// driver sources. The count keeps same-named methods of
/// other types (`Application::query(query)`, a runtime's `prepare` of four
/// arguments, an HTTP client's `execute(request)`) out of the method-call
/// rule.
pub const DRIVER_METHODS: &[(&str, usize)] = &[
    ("query", 2),
    ("query_one", 2),
    ("query_opt", 2),
    ("query_scalar", 2),
    ("query_one_scalar", 2),
    ("query_opt_scalar", 2),
    ("query_raw", 2),
    ("query_typed", 2),
    ("query_typed_one", 2),
    ("query_typed_opt", 2),
    ("query_typed_raw", 2),
    ("query_portal", 2),
    ("query_portal_raw", 2),
    ("execute", 2),
    ("execute_typed", 2),
    ("execute_raw", 2),
    ("batch_execute", 1),
    ("simple_query", 1),
    ("simple_query_raw", 1),
    ("prepare", 1),
    ("prepare_typed", 2),
    ("prepare_cached", 1),
    ("prepare_typed_cached", 2),
    ("bind", 2),
    ("bind_raw", 2),
    ("copy_in", 1),
    ("copy_out", 1),
];

/// Types and traits whose [`DRIVER_METHODS`] are the driver's:
/// `tokio_postgres`' `Client`, `Transaction` and `GenericClient`,
/// `deadpool_postgres`' `Object`, `ClientWrapper` and `Transaction`, and
/// the pool's `PooledClient`, `Tx` and `DetachedClient`, which deref to
/// them.
pub const DRIVER_TYPES: &[&str] = &[
    "Client",
    "Transaction",
    "GenericClient",
    "Object",
    "ClientWrapper",
    "PooledClient",
    "Tx",
    "DetachedClient",
];

/// Crates every path through which reaches the driver.
pub const DRIVER_CRATES: &[&str] = &["tokio_postgres", "deadpool_postgres"];

fn arity(method: &str) -> Option<usize> {
    DRIVER_METHODS
        .iter()
        .find(|(name, _)| *name == method)
        .map(|(_, arity)| *arity)
}

/// Parse a source file. Every workspace file compiles, so it parses.
pub fn parse(source: &str) -> syn::Result<syn::File> {
    syn::parse_file(source)
}

/// The constants `file` declares with the type `Statement`
/// (`const PING: Statement = Statement::new(…)`), by name.
pub fn statement_constants(file: &syn::File) -> BTreeSet<String> {
    struct Constants(BTreeSet<String>);
    impl Visit<'_> for Constants {
        fn visit_item_const(&mut self, item: &syn::ItemConst) {
            if is_statement(&item.ty) {
                self.0.insert(item.ident.to_string());
            }
            visit::visit_item_const(self, item);
        }
        fn visit_impl_item_const(&mut self, item: &syn::ImplItemConst) {
            if is_statement(&item.ty) {
                self.0.insert(item.ident.to_string());
            }
            visit::visit_impl_item_const(self, item);
        }
    }
    let mut constants = Constants(BTreeSet::new());
    constants.visit_file(file);
    constants.0
}

/// Whether `ty` names `Statement`.
fn is_statement(ty: &Type) -> bool {
    matches!(ty, Type::Path(p) if p.qself.is_none()
        && p.path.segments.last().is_some_and(|s| s.ident == "Statement"))
}

/// Every unregistered call in `file`, as `(line, what)` sorted by line:
/// `what` is the method (`execute`), the path (`Transaction::execute`) or
/// the metavariable (`$m`). `statements` are the registered constant
/// names, from [`statement_constants`] over every scanned file.
pub fn unregistered_calls(file: &syn::File, statements: &BTreeSet<String>) -> Vec<(usize, String)> {
    let mut scan = Scan {
        statements,
        values: values(file),
        driver_names: driver_names(file),
        found: Vec::new(),
    };
    scan.visit_file(file);
    scan.found.sort();
    scan.found
}

/// The constants and statics `file` declares with another type than
/// `Statement`: a name another file gives a statement is not one here.
fn values(file: &syn::File) -> BTreeSet<String> {
    struct Values(BTreeSet<String>);
    impl Visit<'_> for Values {
        fn visit_item_const(&mut self, item: &syn::ItemConst) {
            if !is_statement(&item.ty) {
                self.0.insert(item.ident.to_string());
            }
            visit::visit_item_const(self, item);
        }
        fn visit_impl_item_const(&mut self, item: &syn::ImplItemConst) {
            if !is_statement(&item.ty) {
                self.0.insert(item.ident.to_string());
            }
            visit::visit_impl_item_const(self, item);
        }
        fn visit_item_static(&mut self, item: &syn::ItemStatic) {
            self.0.insert(item.ident.to_string());
            visit::visit_item_static(self, item);
        }
    }
    let mut values = Values(BTreeSet::new());
    values.visit_file(file);
    values.0
}

/// [`DRIVER_TYPES`] and [`DRIVER_CRATES`] plus the names `file` binds to
/// one of them: `use tokio_postgres::GenericClient as G`, `type C =
/// tokio_postgres::Client`, and generic parameters bounded by one
/// (`<C: GenericClient>`, `where C: GenericClient`).
fn driver_names(file: &syn::File) -> BTreeSet<String> {
    struct Names(BTreeSet<String>);
    impl Names {
        fn bounded(&self, bounds: &Punctuated<syn::TypeParamBound, Token![+]>) -> bool {
            bounds.iter().any(|bound| {
                matches!(bound, syn::TypeParamBound::Trait(t)
                    if t.path.segments.iter().any(|s| self.0.contains(&s.ident.to_string())))
            })
        }
    }
    impl Visit<'_> for Names {
        fn visit_type_param(&mut self, param: &syn::TypeParam) {
            if self.bounded(&param.bounds) {
                self.0.insert(param.ident.to_string());
            }
            visit::visit_type_param(self, param);
        }
        fn visit_predicate_type(&mut self, predicate: &syn::PredicateType) {
            if let Type::Path(p) = &predicate.bounded_ty
                && p.qself.is_none()
                && let Some(ident) = p.path.get_ident()
                && self.bounded(&predicate.bounds)
            {
                self.0.insert(ident.to_string());
            }
            visit::visit_predicate_type(self, predicate);
        }
        fn visit_use_rename(&mut self, rename: &syn::UseRename) {
            if self.0.contains(&rename.ident.to_string()) {
                self.0.insert(rename.rename.to_string());
            }
        }
        fn visit_item_type(&mut self, item: &syn::ItemType) {
            if let Type::Path(p) = &*item.ty
                && p.path
                    .segments
                    .iter()
                    .any(|s| self.0.contains(&s.ident.to_string()))
            {
                self.0.insert(item.ident.to_string());
            }
            visit::visit_item_type(self, item);
        }
    }
    let mut names = Names(
        DRIVER_TYPES
            .iter()
            .chain(DRIVER_CRATES)
            .map(|name| (*name).to_owned())
            .collect(),
    );
    // Twice, so an alias of an alias declared later in the file resolves.
    names.visit_file(file);
    names.visit_file(file);
    names.0
}

struct Scan<'a> {
    statements: &'a BTreeSet<String>,
    /// [`values`]: names that are no statement in this file.
    values: BTreeSet<String>,
    driver_names: BTreeSet<String>,
    found: Vec<(usize, String)>,
}

impl Scan<'_> {
    fn flag(&mut self, span: proc_macro2::Span, what: String) {
        self.found.push((span.start().line, what));
    }

    /// `&path::A_STATEMENT`, a registered constant.
    fn registered(&self, argument: Option<&Expr>) -> bool {
        let Some(Expr::Reference(reference)) = argument else {
            return false;
        };
        let Expr::Path(path) = &*reference.expr else {
            return false;
        };
        path.qself.is_none()
            && reference.mutability.is_none()
            && path
                .path
                .segments
                .last()
                .is_some_and(|s| self.statement(&s.ident.to_string()))
    }

    /// A registered statement constant this file does not shadow.
    fn statement(&self, name: &str) -> bool {
        self.statements.contains(name) && !self.values.contains(name)
    }

    /// `T::m` or `<T as Trait>::m` naming a driver method of a driver type.
    fn driver_path(&self, qself: Option<&syn::QSelf>, path: &Path) -> Option<String> {
        let segments: Vec<String> = path.segments.iter().map(|s| s.ident.to_string()).collect();
        let (method, owners) = segments.split_last()?;
        arity(method)?;
        let mut owners: Vec<String> = owners.to_vec();
        if let Some(qself) = qself
            && let Type::Path(ty) = &*qself.ty
        {
            owners.extend(ty.path.segments.iter().map(|s| s.ident.to_string()));
        }
        owners
            .iter()
            .any(|owner| self.driver_names.contains(owner))
            .then(|| segments.join("::"))
    }

    /// The macro's arguments as code when they parse as expressions or
    /// statements, otherwise its tokens.
    fn invocation(&mut self, mac: &Macro) {
        let exprs = Punctuated::<Expr, Token![,]>::parse_terminated;
        if let Ok(exprs) = mac.parse_body_with(exprs) {
            for expr in &exprs {
                self.visit_expr(expr);
            }
        } else if let Ok(stmts) = mac.parse_body_with(syn::Block::parse_within) {
            for stmt in &stmts {
                self.visit_stmt(stmt);
            }
        } else {
            self.tokens(mac.tokens.clone());
        }
    }

    /// The rules on raw tokens: a `macro_rules!` body, or an invocation
    /// whose arguments are not plain code.
    fn tokens(&mut self, stream: TokenStream) {
        let trees: Vec<TokenTree> = stream.into_iter().collect();
        for (at, tree) in trees.iter().enumerate() {
            match tree {
                TokenTree::Group(group) => self.tokens(group.stream()),
                TokenTree::Ident(ident) => {
                    let name = ident.to_string();
                    let Some(arity) = arity(&name) else { continue };
                    if after_dot(&trees, at) {
                        let Some(args) = call_arguments(&trees[at + 1..]) else {
                            continue;
                        };
                        if args.len() == arity && !self.registered_tokens(args.get(1)) {
                            self.flag(ident.span(), name);
                        }
                    } else if let Some(owner) = self.path_owner(&trees, at) {
                        self.flag(ident.span(), format!("{owner}::{name}"));
                    }
                }
                TokenTree::Punct(dollar) if dollar.as_char() == '$' => {
                    // `.$m(…)`, `.$m::<…>(…)` or `<$t>::$m(…)`: a method the
                    // scan cannot name.
                    if let Some(TokenTree::Ident(meta)) = trees.get(at + 1)
                        && (after_dot(&trees, at) || after_path_separator(&trees, at))
                        && call_arguments(&trees[at + 2..]).is_some()
                    {
                        self.flag(meta.span(), format!("${meta}"));
                    }
                }
                TokenTree::Punct(_) | TokenTree::Literal(_) => {}
            }
        }
    }

    /// [`Scan::registered`] on the tokens of one argument.
    fn registered_tokens(&self, argument: Option<&Vec<TokenTree>>) -> bool {
        let Some(argument) = argument else {
            return false;
        };
        let Some((TokenTree::Punct(amp), path)) = argument.split_first() else {
            return false;
        };
        if amp.as_char() != '&' || path.is_empty() {
            return false;
        }
        // `a::b::C`: identifiers separated by `::`.
        let mut last = None;
        for (at, tree) in path.iter().enumerate() {
            match (at % 3, tree) {
                (0, TokenTree::Ident(ident)) => last = Some(ident.to_string()),
                (1 | 2, TokenTree::Punct(p)) if p.as_char() == ':' => {}
                _ => return false,
            }
        }
        path.len() % 3 == 1 && last.is_some_and(|name| self.statement(&name))
    }
}

/// Whether the token before `at` is a lone `.` (not `..`).
fn after_dot(trees: &[TokenTree], at: usize) -> bool {
    let Some(TokenTree::Punct(dot)) = at.checked_sub(1).and_then(|i| trees.get(i)) else {
        return false;
    };
    dot.as_char() == '.'
        && !matches!(
            at.checked_sub(2).and_then(|i| trees.get(i)),
            Some(TokenTree::Punct(p)) if p.as_char() == '.' && p.spacing() == Spacing::Joint
        )
}

/// The arguments of a call starting at `rest` (`(…)` or `::<…>(…)`),
/// split on top-level commas.
fn call_arguments(rest: &[TokenTree]) -> Option<Vec<Vec<TokenTree>>> {
    let mut rest = rest;
    if let [
        TokenTree::Punct(a),
        TokenTree::Punct(b),
        TokenTree::Punct(open),
        tail @ ..,
    ] = rest
        && a.as_char() == ':'
        && b.as_char() == ':'
        && open.as_char() == '<'
    {
        let mut depth = 1usize;
        let mut end = None;
        for (at, tree) in tail.iter().enumerate() {
            if let TokenTree::Punct(p) = tree {
                match p.as_char() {
                    '<' => depth += 1,
                    // `->` inside a `Fn(…) -> T` bound closes nothing.
                    '>' if !(at > 0
                        && matches!(&tail[at - 1], TokenTree::Punct(m) if m.as_char() == '-')) =>
                    {
                        depth -= 1;
                        if depth == 0 {
                            end = Some(at);
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }
        rest = &tail[end? + 1..];
    }
    let Some(TokenTree::Group(group)) = rest.first() else {
        return None;
    };
    if group.delimiter() != Delimiter::Parenthesis {
        return None;
    }
    let mut args = vec![Vec::new()];
    for tree in group.stream() {
        match &tree {
            TokenTree::Punct(p) if p.as_char() == ',' => args.push(Vec::new()),
            _ => args.last_mut().expect("one argument at least").push(tree),
        }
    }
    if args.last().is_some_and(Vec::is_empty) {
        args.pop();
    }
    Some(args)
}

/// Whether the two tokens before `at` are `::`.
fn after_path_separator(trees: &[TokenTree], at: usize) -> bool {
    at >= 2
        && matches!((&trees[at - 2], &trees[at - 1]),
            (TokenTree::Punct(a), TokenTree::Punct(b)) if a.as_char() == ':' && b.as_char() == ':')
}

impl Scan<'_> {
    /// The owner that makes `trees[at]`, a [`DRIVER_METHODS`] name ending a
    /// path, the driver's: a driver name among the segments before it
    /// (`a::Transaction::execute`), a metavariable leading them
    /// (`$t::execute`), or either inside a qualified type
    /// (`<Transaction as Run>::execute`, `<$t>::execute`).
    fn path_owner(&self, trees: &[TokenTree], at: usize) -> Option<String> {
        let mut end = at;
        let mut owners = Vec::new();
        while after_path_separator(trees, end) {
            match end.checked_sub(3).map(|i| &trees[i]) {
                Some(TokenTree::Ident(owner)) => {
                    owners.insert(0, owner.to_string());
                    end -= 3;
                }
                Some(TokenTree::Punct(close)) if close.as_char() == '>' && owners.is_empty() => {
                    return self.qualified_owner(&trees[..end - 2]);
                }
                _ => break,
            }
        }
        let metavariable = end
            .checked_sub(1)
            .is_some_and(|i| matches!(&trees[i], TokenTree::Punct(p) if p.as_char() == '$'));
        match owners.first() {
            Some(first) if metavariable => Some(format!("${first}")),
            _ => owners
                .into_iter()
                .find(|owner| self.driver_names.contains(owner)),
        }
    }

    /// The driver name or metavariable inside the `<…>` that `before` ends
    /// with.
    fn qualified_owner(&self, before: &[TokenTree]) -> Option<String> {
        let mut depth = 0usize;
        let mut owner = None;
        for at in (0..before.len()).rev() {
            match &before[at] {
                TokenTree::Punct(p) if p.as_char() == '>' => depth += 1,
                TokenTree::Punct(p) if p.as_char() == '<' => {
                    depth -= 1;
                    if depth == 0 {
                        return owner;
                    }
                }
                TokenTree::Ident(ident) if owner.is_none() => {
                    let dollar = at.checked_sub(1).is_some_and(
                        |i| matches!(&before[i], TokenTree::Punct(p) if p.as_char() == '$'),
                    );
                    if dollar {
                        owner = Some(format!("${ident}"));
                    } else if self.driver_names.contains(&ident.to_string()) {
                        owner = Some(ident.to_string());
                    }
                }
                _ => {}
            }
        }
        None
    }
}

impl<'ast> Visit<'ast> for Scan<'_> {
    fn visit_expr_method_call(&mut self, call: &'ast ExprMethodCall) {
        let method = call.method.to_string();
        if arity(&method) == Some(call.args.len()) && !self.registered(call.args.iter().nth(1)) {
            self.flag(call.method.span(), method);
        }
        visit::visit_expr_method_call(self, call);
    }
    fn visit_expr_call(&mut self, call: &'ast ExprCall) {
        // `T::m(…)` is one finding: its path is not a method reference too.
        if let Expr::Path(func) = &*call.func
            && let Some(what) = self.driver_path(func.qself.as_ref(), &func.path)
        {
            let span = func.path.segments.last().map(|s| s.ident.span());
            self.flag(span.unwrap_or_else(proc_macro2::Span::call_site), what);
            for arg in &call.args {
                self.visit_expr(arg);
            }
            return;
        }
        visit::visit_expr_call(self, call);
    }
    fn visit_expr_path(&mut self, path: &'ast ExprPath) {
        if let Some(what) = self.driver_path(path.qself.as_ref(), &path.path) {
            let span = path.path.segments.last().map(|s| s.ident.span());
            self.flag(span.unwrap_or_else(proc_macro2::Span::call_site), what);
        }
        visit::visit_expr_path(self, path);
    }
    fn visit_macro(&mut self, mac: &'ast Macro) {
        if mac.path.is_ident("macro_rules") {
            self.tokens(mac.tokens.clone());
        } else {
            self.invocation(mac);
        }
    }
}
