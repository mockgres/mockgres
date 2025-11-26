use super::{DataType, Field, IdentitySpec, Schema, Value};
use crate::catalog::{QualifiedName, SchemaName, TableId};
use std::fmt;

#[derive(Clone, Copy, Debug)]
pub enum CmpOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Clone, Debug)]
pub enum Expr {
    Literal(Value),
    Column(usize),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnRefName {
    pub schema: Option<String>,
    pub relation: Option<String>,
    pub column: String,
}

impl fmt::Display for ColumnRefName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(schema) = &self.schema {
            write!(f, "{schema}.")?;
        }
        if let Some(rel) = &self.relation {
            write!(f, "{rel}.")?;
        }
        write!(f, "{}", self.column)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ScalarExpr {
    Literal(Value),
    Column(ColumnRefName),
    ColumnIdx(usize),
    Cast {
        expr: Box<ScalarExpr>,
        ty: DataType,
    },
    Param {
        idx: usize,
        ty: Option<DataType>,
    },
    BinaryOp {
        op: ScalarBinaryOp,
        left: Box<ScalarExpr>,
        right: Box<ScalarExpr>,
    },
    UnaryOp {
        op: ScalarUnaryOp,
        expr: Box<ScalarExpr>,
    },
    Func {
        func: ScalarFunc,
        args: Vec<ScalarExpr>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ScalarBinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Concat,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ScalarUnaryOp {
    Negate,
}

/// Supported scalar functions.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ScalarFunc {
    Coalesce,
    Upper,
    Lower,
    Length,
    CurrentSchema,
    CurrentSchemas,
    CurrentDatabase,
    Now,
    CurrentTimestamp,
    StatementTimestamp,
    TransactionTimestamp,
    ClockTimestamp,
    CurrentDate,
}

/// Logical aggregate function identifiers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Planner representation of an aggregate invocation.
#[derive(Clone, Debug)]
pub struct AggCall {
    pub func: AggFunc,
    pub expr: Option<ScalarExpr>,
}

#[derive(Clone, Debug)]
pub enum BoolExpr {
    Literal(bool),
    Comparison {
        lhs: ScalarExpr,
        op: CmpOp,
        rhs: ScalarExpr,
    },
    And(Vec<BoolExpr>),
    Or(Vec<BoolExpr>),
    Not(Box<BoolExpr>),
    IsNull {
        expr: ScalarExpr,
        negated: bool,
    },
}

#[derive(Clone, Debug)]
pub enum InsertSource {
    Expr(ScalarExpr),
    Default,
}

#[derive(Clone, Debug)]
pub enum ReturningExpr {
    Star,
    Expr { expr: ScalarExpr, alias: String },
}

#[derive(Clone, Debug)]
pub struct ReturningClause {
    pub exprs: Vec<ReturningExpr>,
}

#[derive(Clone, Debug)]
pub enum Selection {
    Star,
    Columns(Vec<String>),
}

pub type ObjName = QualifiedName;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReferentialAction {
    Restrict,
    Cascade,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DbDdlKind {
    Create,
    Drop,
    Alter,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockMode {
    Update,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LockRequest {
    pub mode: LockMode,
    pub skip_locked: bool,
    pub nowait: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LockSpec {
    pub mode: LockMode,
    pub skip_locked: bool,
    pub nowait: bool,
    pub target: TableId,
}

#[derive(Clone, Debug)]
pub struct PrimaryKeySpec {
    pub name: Option<String>,
    pub columns: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct UniqueSpec {
    pub name: Option<String>,
    pub columns: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ForeignKeySpec {
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub referenced_table: ObjName,
    pub referenced_columns: Option<Vec<String>>,
    pub on_delete: ReferentialAction,
}

#[derive(Clone, Debug)]
pub enum Plan {
    Values {
        rows: Vec<Vec<Expr>>,
        schema: Schema,
    },
    UnboundSeqScan {
        table: ObjName,
        alias: Option<String>,
        selection: Selection,
        lock: Option<LockRequest>,
    },
    Filter {
        input: Box<Plan>,
        expr: BoolExpr,
        project_prefix_len: Option<usize>,
    },
    Order {
        input: Box<Plan>,
        keys: Vec<SortKey>,
    },
    Limit {
        input: Box<Plan>,
        limit: Option<usize>,
        offset: usize,
    },
    UnboundJoin {
        left: Box<Plan>,
        right: Box<Plan>,
    },
    Join {
        left: Box<Plan>,
        right: Box<Plan>,
        schema: Schema,
    },
    SeqScan {
        table: ObjName,
        cols: Vec<(usize, Field)>,
        schema: Schema,
        lock: Option<LockSpec>,
    },
    LockRows {
        table: ObjName,
        input: Box<Plan>,
        lock: LockSpec,
        row_id_idx: usize,
        schema: Schema,
    },
    Projection {
        input: Box<Plan>,
        exprs: Vec<(ScalarExpr, String)>,
        schema: Schema,
    },
    /// Hash-based aggregate node (group keys first, followed by aggregate outputs).
    Aggregate {
        input: Box<Plan>,
        group_exprs: Vec<(ScalarExpr, String)>,
        agg_exprs: Vec<(AggCall, String)>,
        schema: Schema,
    },
    CountRows {
        input: Box<Plan>,
        schema: Schema,
    },
    CreateTable {
        table: ObjName,
        cols: Vec<(
            String,
            DataType,
            bool,
            Option<ScalarExpr>,
            Option<IdentitySpec>,
        )>,
        pk: Option<PrimaryKeySpec>,
        foreign_keys: Vec<ForeignKeySpec>,
        uniques: Vec<UniqueSpec>,
    },
    AlterTableAddColumn {
        table: ObjName,
        column: (
            String,
            DataType,
            bool,
            Option<ScalarExpr>,
            Option<IdentitySpec>,
        ),
        if_not_exists: bool,
    },
    AlterTableDropColumn {
        table: ObjName,
        column: String,
        if_exists: bool,
    },
    AlterTableAddConstraintUnique {
        table: ObjName,
        name: Option<String>,
        columns: Vec<String>,
    },
    AlterTableDropConstraint {
        table: ObjName,
        name: String,
        if_exists: bool,
    },
    CreateIndex {
        name: String,
        table: ObjName,
        columns: Vec<String>,
        if_not_exists: bool,
        is_unique: bool,
    },
    DropIndex {
        indexes: Vec<ObjName>,
        if_exists: bool,
    },
    DropTable {
        tables: Vec<ObjName>,
        if_exists: bool,
    },
    CreateSchema {
        name: SchemaName,
        if_not_exists: bool,
    },
    DropSchema {
        schemas: Vec<SchemaName>,
        if_exists: bool,
        cascade: bool,
    },
    AlterSchemaRename {
        name: SchemaName,
        new_name: SchemaName,
    },
    CreateDatabase {
        name: String,
    },
    DropDatabase {
        name: String,
    },
    AlterDatabase {
        name: String,
    },
    UnsupportedDbDDL {
        kind: DbDdlKind,
        name: String,
    },
    ShowVariable {
        name: String,
        schema: Schema,
    },
    SetVariable {
        name: String,
        value: Option<Vec<String>>,
    },
    InsertValues {
        table: ObjName,
        columns: Option<Vec<String>>,
        rows: Vec<Vec<InsertSource>>,
        override_system_value: bool,
        on_conflict: Option<OnConflictAction>,
        returning: Option<ReturningClause>,
        returning_schema: Option<Schema>,
    },
    Update {
        table: ObjName,
        sets: Vec<UpdateSet>,
        filter: Option<BoolExpr>,
        returning: Option<ReturningClause>,
        returning_schema: Option<Schema>,
    },
    Delete {
        table: ObjName,
        filter: Option<BoolExpr>,
        returning: Option<ReturningClause>,
        returning_schema: Option<Schema>,
    },
    BeginTransaction,
    CommitTransaction,
    RollbackTransaction,
}

#[derive(Clone, Debug)]
pub enum UpdateSet {
    ByName(String, ScalarExpr),
    ByIndex(usize, ScalarExpr),
}

#[derive(Clone, Debug)]
pub enum SortKey {
    ByName {
        col: String,
        asc: bool,
        nulls_first: Option<bool>,
    },
    ByIndex {
        idx: usize,
        asc: bool,
        nulls_first: Option<bool>,
    },
    Expr {
        expr: ScalarExpr,
        asc: bool,
        nulls_first: Option<bool>,
    },
}

impl Plan {
    pub fn schema(&self) -> &Schema {
        match self {
            Plan::Values { schema, .. }
            | Plan::Aggregate { schema, .. }
            | Plan::SeqScan { schema, .. }
            | Plan::LockRows { schema, .. }
            | Plan::Projection { schema, .. }
            | Plan::CountRows { schema, .. }
            | Plan::Join { schema, .. } => schema,
            Plan::ShowVariable { schema, .. } => schema,
            Plan::Filter { input, .. } | Plan::Order { input, .. } | Plan::Limit { input, .. } => {
                input.schema()
            }
            Plan::InsertValues {
                on_conflict: _,
                returning_schema,
                ..
            }
            | Plan::Update {
                returning_schema, ..
            }
            | Plan::Delete {
                returning_schema, ..
            } => {
                static EMPTY: Schema = Schema { fields: vec![] };
                returning_schema.as_ref().unwrap_or(&EMPTY)
            }
            Plan::UnboundSeqScan { .. }
            | Plan::UnboundJoin { .. }
            | Plan::CreateTable { .. }
            | Plan::AlterTableAddColumn { .. }
            | Plan::AlterTableDropColumn { .. }
            | Plan::AlterTableAddConstraintUnique { .. }
            | Plan::AlterTableDropConstraint { .. }
            | Plan::CreateIndex { .. }
            | Plan::DropIndex { .. }
            | Plan::DropTable { .. }
            | Plan::CreateSchema { .. }
            | Plan::DropSchema { .. }
            | Plan::AlterSchemaRename { .. }
            | Plan::CreateDatabase { .. }
            | Plan::DropDatabase { .. }
            | Plan::AlterDatabase { .. }
            | Plan::UnsupportedDbDDL { .. }
            | Plan::SetVariable { .. }
            | Plan::BeginTransaction
            | Plan::CommitTransaction
            | Plan::RollbackTransaction => {
                static EMPTY: Schema = Schema { fields: vec![] };
                &EMPTY
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum OnConflictTarget {
    /// ON CONFLICT DO NOTHING (no explicit target)
    None,
    /// ON CONFLICT (col1, col2, ...)
    Columns(Vec<String>),
    /// ON CONFLICT ON CONSTRAINT constraint_name
    Constraint(String),
}

#[derive(Clone, Debug)]
pub enum OnConflictAction {
    DoNothing { target: OnConflictTarget },
    // DO UPDATE will be added later (Phase 2)
}
