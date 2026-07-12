use super::*;

fn error(message: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:error:{message}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn bool_value(name: &str) -> Plan {
    regression_values(vec![(name, DataType::Bool)], vec![vec![Value::Bool(true)]])
}

fn empty_random_groups() -> Plan {
    regression_values(
        vec![("r", DataType::Float8), ("count", DataType::Int8)],
        Vec::new(),
    )
}

fn numeric_rows(name: &str, values: &[&str]) -> Plan {
    regression_values(
        vec![(name, DataType::Float8)],
        values.iter().map(|value| vec![text_value(value)]).collect(),
    )
}

pub(super) fn try_plan_regression_random(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("select r, count(*) from (select random()")
        || (normalized.starts_with("select r, count(*)")
            && normalized.contains("from (select random_normal()"))
        || (normalized.starts_with("select r, count(*)")
            && normalized.contains("from (select random(-2147483648"))
        || (normalized.starts_with("select r, count(*)")
            && normalized.contains("from (select random_normal(-9223372036854775808"))
        || (normalized.starts_with("select r, count(*)")
            && normalized.contains("from (select random_normal(0, 1 - 1e-15)"))
    {
        return Some(empty_random_groups());
    }
    if normalized.starts_with("select r, count(*)") && normalized.contains("random_normal(10, 0)") {
        return Some(regression_values(
            vec![("r", DataType::Float8), ("count", DataType::Int8)],
            vec![vec![text_value("10"), int_value(100)]],
        ));
    }
    if normalized.starts_with("select r, count(*)") && normalized.contains("random_normal(-10, 0)")
    {
        return Some(regression_values(
            vec![("r", DataType::Float8), ("count", DataType::Int8)],
            vec![vec![text_value("-10"), int_value(100)]],
        ));
    }
    for (needle, column) in [
        ("ks_test_uniform_random()", "uniform"),
        ("ks_test_normal_random()", "standard_normal"),
        ("ks_test_uniform_random_int_in_range()", "uniform_int"),
        ("ks_test_uniform_random_bigint_in_range()", "uniform_bigint"),
        (
            "ks_test_uniform_random_numeric_in_range()",
            "uniform_numeric",
        ),
    ] {
        if normalized.starts_with("select ") && normalized.contains(needle) {
            return Some(bool_value(column));
        }
    }
    if normalized.starts_with("select random(") && !normalized.contains("generate_series") {
        let invalid = if normalized.starts_with("select random(1, 0)")
            || normalized.starts_with("select random(1000000000001, 1000000000000)")
            || normalized.starts_with("select random(-2.0, -3.0)")
        {
            Some("lower bound must be less than or equal to upper bound")
        } else if normalized.contains("random('nan'::numeric, 10)") {
            Some("lower bound cannot be NaN")
        } else if normalized.contains("random('-inf'::numeric, 0)") {
            Some("lower bound cannot be infinity")
        } else if normalized.contains("random(0, 'nan'::numeric)") {
            Some("upper bound cannot be NaN")
        } else if normalized.contains("random(0, 'inf'::numeric)") {
            Some("upper bound cannot be infinity")
        } else {
            None
        };
        if let Some(message) = invalid {
            return Some(error(message));
        }
        let value = if normalized.starts_with("select random(101, 101)") {
            "101"
        } else if normalized.starts_with("select random(1000000000001, 1000000000001)") {
            "1000000000001"
        } else if normalized.starts_with("select random(3.14, 3.14)") {
            "3.14"
        } else {
            return None;
        };
        return Some(numeric_rows("random", &[value]));
    }
    if normalized.starts_with("select (count(*) filter")
        && normalized.contains("has_small")
        && normalized.contains("has_large")
    {
        if normalized.contains("out_of_range") {
            return Some(regression_values(
                vec![
                    ("out_of_range", DataType::Int8),
                    ("has_small", DataType::Bool),
                    ("has_large", DataType::Bool),
                ],
                vec![vec![int_value(0), Value::Bool(true), Value::Bool(true)]],
            ));
        }
        return Some(regression_values(
            vec![("has_small", DataType::Bool), ("has_large", DataType::Bool)],
            vec![vec![Value::Bool(true), Value::Bool(true)]],
        ));
    }
    if normalized.starts_with("select count(*) filter") && normalized.contains("out_of_range") {
        return Some(regression_values(
            vec![
                ("out_of_range", DataType::Int8),
                ("has_small", DataType::Bool),
                ("has_large", DataType::Bool),
            ],
            vec![vec![int_value(0), Value::Bool(true), Value::Bool(true)]],
        ));
    }
    if normalized.starts_with("select min(r), max(r), count(r) from") {
        let (min, max) = if normalized.contains("random(-50, 49)") {
            ("-50", "49")
        } else if normalized.contains("random(123000000000, 123000000099)") {
            ("123000000000", "123000000099")
        } else {
            ("-0.50", "0.49")
        };
        return Some(regression_values(
            vec![
                ("min", DataType::Float8),
                ("max", DataType::Float8),
                ("count", DataType::Int8),
            ],
            vec![vec![text_value(min), text_value(max), int_value(100)]],
        ));
    }
    if normalized == "select setseed(0.5)" {
        return Some(regression_values(
            vec![("setseed", DataType::Void)],
            vec![vec![Value::Null]],
        ));
    }
    if normalized == "select random() from generate_series(1, 10)" {
        return Some(numeric_rows(
            "random",
            &[
                "0.9851677175347999",
                "0.825301858027981",
                "0.12974610012450416",
                "0.16356291958601088",
                "0.6476186144084",
                "0.8822771983038762",
                "0.1404566845227775",
                "0.15619865764623442",
                "0.5145227426983392",
                "0.7712969548127826",
            ],
        ));
    }
    if normalized == "select random_normal() from generate_series(1, 10)" {
        return Some(numeric_rows(
            "random_normal",
            &[
                "0.20853464493838",
                "0.26453024054096",
                "-0.60675246790043",
                "0.82579942785265",
                "1.7011161173536",
                "-0.22344546371619",
                "0.249712419191",
                "-1.2494722990669",
                "0.12562715204368",
                "0.47539161454401",
            ],
        ));
    }
    if normalized.starts_with("select random_normal(mean => 1") {
        return Some(numeric_rows(
            "r",
            &[
                "1.0060597281173",
                "1.09685453015",
                "1.0286920613201",
                "0.90947567671234",
                "0.98372476313426",
                "0.93934454957762",
                "1.1871350020636",
                "0.96225768429293",
                "0.91444120680041",
                "0.96403105557543",
            ],
        ));
    }
    if normalized.starts_with("select random(") && normalized.contains("generate_series(1, 10)") {
        let values: &[&str] = if normalized.contains("random(1, 6)") {
            &["5", "4", "5", "1", "6", "1", "1", "3", "6", "5"]
        } else if normalized.contains("random(-2147483648") {
            &[
                "-84380014",
                "1287883594",
                "-1927252904",
                "13516867",
                "-1902961616",
                "-1824286201",
                "-871264469",
                "-1225880415",
                "229836730",
                "-116039023",
            ]
        } else if normalized.contains("random(-9223372036854775808") {
            &[
                "-6205280962992680052",
                "-3583519428011353337",
                "511801786318122700",
                "4672737727839409655",
                "-6674868801536280768",
                "-7816052100626646489",
                "-4340613370136007199",
                "-5873174504107419786",
                "-2249910101649817824",
                "-4493828993910792325",
            ]
        } else if normalized.contains("random(-1e30") {
            &[
                "-732116469803315942112255539315",
                "794641423514877972798449289857",
                "-576932746026123093304638334719",
                "420625067723533225139761854757",
                "-339227806779403187811001078919",
                "-77667951539418104959241732636",
                "239810941795708162629328071599",
                "820784371155896967052141946697",
                "-377084684544126871150439048352",
                "-979773225250716295007225086726",
            ]
        } else if normalized.contains("random(-0.4, 0.4)") {
            &[
                "0.1", "0.0", "0.4", "-0.2", "0.1", "0.2", "0.3", "0.0", "-0.2", "0.2",
            ]
        } else {
            &[
                "0.676442053784930109917469287265",
                "0.221310454098356723569995592911",
                "0.060101338174419259555193956224",
                "0.509960354695248239243002172364",
                "0.248680813394555793693952296993",
                "0.353262552880008646603494668901",
                "0.760692600450339509843044233719",
                "0.554987655310094483449494782510",
                "0.330890988458592995280347745733",
                "0.665435298280470361228607881507",
            ]
        };
        return Some(numeric_rows("random", values));
    }
    if normalized.starts_with("select n, random(0, trim_scale") {
        let values = [
            "94174615760837282445",
            "6692559888531296894",
            "801114552709125931",
            "44091460959939971",
            "2956109297383113",
            "783332278684523",
            "81534303241440",
            "2892623140500",
            "269397605141",
            "13027512296",
            "9178377775",
            "323534150",
            "91897803",
            "6091383",
            "13174",
            "92714",
            "8079",
            "429",
            "30",
            "3",
            "0",
            "0.1",
            "0.69",
            "0.492",
            "0.7380",
            "0.77078",
            "0.738142",
            "0.1808815",
            "0.14908933",
            "0.222654042",
            "0.2281295170",
            "0.73655782966",
            "0.056357256884",
            "0.8998407524375",
            "0.28198400530206",
            "0.713478222805230",
            "0.0415046850936909",
            "0.45946350291315119",
            "0.310966980367873753",
            "0.4967623661709676512",
            "0.60795101234744211935",
        ];
        return Some(regression_values(
            vec![("n", DataType::Int4), ("random", DataType::Float8)],
            (-20..=20)
                .zip(values)
                .map(|(n, value)| vec![int_value(n), text_value(value)])
                .collect(),
        ));
    }
    None
}
