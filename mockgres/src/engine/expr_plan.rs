use super::{DataType, Field, Schema, Value};

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

#[derive(Clone, Debug)]
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

#[derive(Clone, Copy, Debug)]
pub enum ScalarBinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Concat,
}

#[derive(Clone, Copy, Debug)]
pub enum ScalarUnaryOp {
    Negate,
}

#[derive(Clone, Copy, Debug)]
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
pub enum Selection {
    Star,
    Columns(Vec<String>),
}

#[derive(Clone, Debug)]
pub struct ObjName {
    pub schema: Option<String>,
    pub name: String,
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
    SeqScan {
        table: ObjName,
        cols: Vec<(usize, Field)>,
        schema: Schema,
    },
    Projection {
        input: Box<Plan>,
        exprs: Vec<(ScalarExpr, String)>,
        schema: Schema,
    },
    CreateTable {
        table: ObjName,
        cols: Vec<(String, DataType, bool, Option<Value>)>,
        pk: Option<Vec<String>>,
    },
    AlterTableAddColumn {
        table: ObjName,
        column: (String, DataType, bool, Option<Value>),
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
    },
    Update {
        table: ObjName,
        sets: Vec<UpdateSet>,
        filter: Option<BoolExpr>,
    },
    Delete {
        table: ObjName,
        filter: Option<BoolExpr>,
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
            | Plan::Projection { schema, .. } => schema,
            Plan::ShowVariable { schema, .. } => schema,
            Plan::Filter { input, .. } | Plan::Order { input, .. } | Plan::Limit { input, .. } => {
                input.schema()
            }
            Plan::UnboundSeqScan { .. }
            | Plan::CreateTable { .. }
            | Plan::AlterTableAddColumn { .. }
            | Plan::AlterTableDropColumn { .. }
            | Plan::CreateIndex { .. }
            | Plan::DropIndex { .. }
            | Plan::SetVariable { .. }
            | Plan::InsertValues { .. }
            | Plan::Update { .. }
            | Plan::Delete { .. }
            | Plan::BeginTransaction
            | Plan::CommitTransaction
            | Plan::RollbackTransaction => {
                static EMPTY: Schema = Schema { fields: vec![] };
                &EMPTY
            }
        }
    }
}
