use crate::engine::Plan;

#[derive(Clone, Debug)]
pub enum StatementPlan {
    Single(Plan),
    Batch(Vec<Plan>),
}

impl StatementPlan {
    pub fn from_plans(plans: Vec<Plan>) -> Self {
        if plans.len() == 1 {
            Self::Single(plans.into_iter().next().expect("single plan"))
        } else {
            Self::Batch(plans)
        }
    }

    pub fn first_non_empty(&self) -> Option<&Plan> {
        match self {
            Self::Single(plan) => (!matches!(plan, Plan::Empty)).then_some(plan),
            Self::Batch(plans) => plans.iter().find(|plan| !matches!(plan, Plan::Empty)),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.first_non_empty().is_none()
    }

    pub fn single_non_empty(&self) -> Option<&Plan> {
        match self {
            Self::Single(plan) => (!matches!(plan, Plan::Empty)).then_some(plan),
            Self::Batch(plans) => {
                let mut non_empty = plans.iter().filter(|plan| !matches!(plan, Plan::Empty));
                let first = non_empty.next()?;
                if non_empty.next().is_some() {
                    return None;
                }
                Some(first)
            }
        }
    }

    pub fn is_multi_non_empty(&self) -> bool {
        match self {
            Self::Single(_) => false,
            Self::Batch(plans) => {
                plans
                    .iter()
                    .filter(|plan| !matches!(plan, Plan::Empty))
                    .take(2)
                    .count()
                    > 1
            }
        }
    }
}
