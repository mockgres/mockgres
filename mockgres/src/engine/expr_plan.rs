use super::{DataType, Field, Schema, Value};
use crate::catalog::TableId;

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
pub enum ScalarExpr {
    Literal(Value),
    Column(String),
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ScalarFunc {
    Coalesce,
    Upper,
    Lower,
    Length,
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

#[derive(Clone, Debug)]
pub struct ObjName {
    pub schema: Option<String>,
    pub name: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReferentialAction {
    Restrict,
    Cascade,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockMode {
    Update,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LockRequest {
    pub mode: LockMode,
    pub skip_locked: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LockSpec {
    pub mode: LockMode,
    pub skip_locked: bool,
    pub target: TableId,
}

#[derive(Clone, Debug)]
pub struct PrimaryKeySpec {
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
    CountRows {
        input: Box<Plan>,
        schema: Schema,
    },
    CreateTable {
        table: ObjName,
        cols: Vec<(String, DataType, bool, Option<ScalarExpr>)>,
        pk: Option<PrimaryKeySpec>,
        foreign_keys: Vec<ForeignKeySpec>,
    },
    AlterTableAddColumn {
        table: ObjName,
        column: (String, DataType, bool, Option<ScalarExpr>),
        if_not_exists: bool,
    },
    AlterTableDropColumn {
        table: ObjName,
        column: String,
        if_exists: bool,
    },
    CreateIndex {
        name: String,
        table: ObjName,
        columns: Vec<String>,
        if_not_exists: bool,
    },
    DropIndex {
        indexes: Vec<ObjName>,
        if_exists: bool,
    },
    DropTable {
        tables: Vec<ObjName>,
        if_exists: bool,
    },
    ShowVariable {
        name: String,
        schema: Schema,
    },
    SetVariable {
        name: String,
        value: Option<String>,
    },
    InsertValues {
        table: ObjName,
        columns: Option<Vec<String>>,
        rows: Vec<Vec<InsertSource>>,
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
                returning_schema, ..
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
            | Plan::CreateIndex { .. }
            | Plan::DropIndex { .. }
            | Plan::DropTable { .. }
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
