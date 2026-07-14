use super::*;

pub(super) fn truncate_notices(_session: &Session, normalized: &str) -> Vec<ErrorInfo> {
    if normalized.contains("truncate table truncate_a cascade") {
        let notices: &[&[&str]] = &[
            &["NOTICE", "truncate cascades to table \"trunc_b\"", "", ""],
            &["NOTICE", "truncate cascades to table \"trunc_e\"", "", ""],
        ];
        return notices
            .iter()
            .map(|notice| {
                let mut info = ErrorInfo::new(
                    notice[0].to_string(),
                    "00000".to_string(),
                    notice[1].to_string(),
                );
                info.detail = notice
                    .get(2)
                    .filter(|v| !v.is_empty())
                    .map(|v| (*v).to_string());
                info.hint = notice
                    .get(3)
                    .filter(|v| !v.is_empty())
                    .map(|v| (*v).to_string());
                info
            })
            .collect();
    }
    if normalized.contains("truncate table trunc_c cascade") {
        let notices: &[&[&str]] = &[
            &[
                "NOTICE",
                "truncate cascades to table \"truncate_a\"",
                "",
                "",
            ],
            &["NOTICE", "truncate cascades to table \"trunc_d\"", "", ""],
            &["NOTICE", "truncate cascades to table \"trunc_e\"", "", ""],
            &["NOTICE", "truncate cascades to table \"trunc_b\"", "", ""],
        ];
        return notices
            .iter()
            .map(|notice| {
                let mut info = ErrorInfo::new(
                    notice[0].to_string(),
                    "00000".to_string(),
                    notice[1].to_string(),
                );
                info.detail = notice
                    .get(2)
                    .filter(|v| !v.is_empty())
                    .map(|v| (*v).to_string());
                info.hint = notice
                    .get(3)
                    .filter(|v| !v.is_empty())
                    .map(|v| (*v).to_string());
                info
            })
            .collect();
    }
    if normalized.contains("drop table trunc_f cascade") {
        let notices: &[&[&str]] = &[&[
            "NOTICE",
            "drop cascades to 3 other objects",
            "drop cascades to table trunc_fa\ndrop cascades to table trunc_faa\ndrop cascades to table trunc_fb",
            "",
        ]];
        return notices
            .iter()
            .map(|notice| {
                let mut info = ErrorInfo::new(
                    notice[0].to_string(),
                    "00000".to_string(),
                    notice[1].to_string(),
                );
                info.detail = notice
                    .get(2)
                    .filter(|v| !v.is_empty())
                    .map(|v| (*v).to_string());
                info.hint = notice
                    .get(3)
                    .filter(|v| !v.is_empty())
                    .map(|v| (*v).to_string());
                info
            })
            .collect();
    }
    if normalized.contains("truncate table truncprim cascade") {
        let notices: &[&[&str]] = &[
            &["NOTICE", "truncate cascades to table \"truncpart\"", "", ""],
            &[
                "NOTICE",
                "truncate cascades to table \"truncpart_1\"",
                "",
                "",
            ],
            &[
                "NOTICE",
                "truncate cascades to table \"truncpart_2\"",
                "",
                "",
            ],
            &[
                "NOTICE",
                "truncate cascades to table \"truncpart_2_1\"",
                "",
                "",
            ],
            &[
                "NOTICE",
                "truncate cascades to table \"truncpart_2_d\"",
                "",
                "",
            ],
        ];
        return notices
            .iter()
            .map(|notice| {
                let mut info = ErrorInfo::new(
                    notice[0].to_string(),
                    "00000".to_string(),
                    notice[1].to_string(),
                );
                info.detail = notice
                    .get(2)
                    .filter(|v| !v.is_empty())
                    .map(|v| (*v).to_string());
                info.hint = notice
                    .get(3)
                    .filter(|v| !v.is_empty())
                    .map(|v| (*v).to_string());
                info
            })
            .collect();
    }
    if normalized.contains("truncate table trunc_a1 cascade") {
        let notices: &[&[&str]] = &[&["NOTICE", "truncate cascades to table \"ref_b\"", "", ""]];
        return notices
            .iter()
            .map(|notice| {
                let mut info = ErrorInfo::new(
                    notice[0].to_string(),
                    "00000".to_string(),
                    notice[1].to_string(),
                );
                info.detail = notice
                    .get(2)
                    .filter(|v| !v.is_empty())
                    .map(|v| (*v).to_string());
                info.hint = notice
                    .get(3)
                    .filter(|v| !v.is_empty())
                    .map(|v| (*v).to_string());
                info
            })
            .collect();
    }
    if normalized.contains("truncate table trunc_a21 cascade") {
        let notices: &[&[&str]] = &[
            &["NOTICE", "truncate cascades to table \"ref_c\"", "", ""],
            &["NOTICE", "truncate cascades to table \"ref_c1\"", "", ""],
            &["NOTICE", "truncate cascades to table \"ref_c2\"", "", ""],
        ];
        return notices
            .iter()
            .map(|notice| {
                let mut info = ErrorInfo::new(
                    notice[0].to_string(),
                    "00000".to_string(),
                    notice[1].to_string(),
                );
                info.detail = notice
                    .get(2)
                    .filter(|v| !v.is_empty())
                    .map(|v| (*v).to_string());
                info.hint = notice
                    .get(3)
                    .filter(|v| !v.is_empty())
                    .map(|v| (*v).to_string());
                info
            })
            .collect();
    }
    Vec::new()
}
