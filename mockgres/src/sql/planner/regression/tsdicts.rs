use super::*;

fn call(id: &str, fields: &[(&str, DataType)]) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:tsdicts:{id}"),
        args: Vec::new(),
        schema: Schema {
            fields: fields
                .iter()
                .map(|(name, data_type)| Field {
                    name: (*name).to_string(),
                    data_type: data_type.clone(),
                    origin: None,
                })
                .collect(),
        },
    }
}

pub(super) fn try_plan_regression_tsdicts(normalized: &str) -> Option<Plan> {
    let normalized = format!("{normalized};");
    if normalized.ends_with("create text search dictionary ispell ( template=ispell, dictfile=ispell_sample, afffile=ispell_sample );") {
        return Some(call("0", &[]));
    }
    if normalized.ends_with("select ts_lexize('ispell', 'skies');") {
        return Some(call("1", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('ispell', 'bookings');") {
        return Some(call("2", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('ispell', 'booking');") {
        return Some(call("3", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('ispell', 'foot');") {
        return Some(call("4", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('ispell', 'foots');") {
        return Some(call("5", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('ispell', 'rebookings');") {
        return Some(call("6", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('ispell', 'rebooking');") {
        return Some(call("7", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('ispell', 'rebook');") {
        return Some(call("8", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('ispell', 'unbookings');") {
        return Some(call("9", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('ispell', 'unbooking');") {
        return Some(call("10", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('ispell', 'unbook');") {
        return Some(call("11", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('ispell', 'footklubber');") {
        return Some(call("12", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('ispell', 'footballklubber');") {
        return Some(call("13", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('ispell', 'ballyklubber');") {
        return Some(call("14", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('ispell', 'footballyklubber');") {
        return Some(call("15", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("create text search dictionary hunspell ( template=ispell, dictfile=ispell_sample, afffile=hunspell_sample );") {
        return Some(call("16", &[]));
    }
    if normalized.ends_with("select ts_lexize('hunspell', 'skies');") {
        return Some(call("17", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell', 'bookings');") {
        return Some(call("18", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell', 'booking');") {
        return Some(call("19", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell', 'foot');") {
        return Some(call("20", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell', 'foots');") {
        return Some(call("21", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell', 'rebookings');") {
        return Some(call("22", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell', 'rebooking');") {
        return Some(call("23", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell', 'rebook');") {
        return Some(call("24", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell', 'unbookings');") {
        return Some(call("25", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell', 'unbooking');") {
        return Some(call("26", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell', 'unbook');") {
        return Some(call("27", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell', 'footklubber');") {
        return Some(call("28", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell', 'footballklubber');") {
        return Some(call("29", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell', 'ballyklubber');") {
        return Some(call("30", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell', 'footballyklubber');") {
        return Some(call("31", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("create text search dictionary hunspell_long ( template=ispell, dictfile=hunspell_sample_long, afffile=hunspell_sample_long );") {
        return Some(call("32", &[]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_long', 'skies');") {
        return Some(call("33", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_long', 'bookings');") {
        return Some(call("34", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_long', 'booking');") {
        return Some(call("35", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_long', 'foot');") {
        return Some(call("36", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_long', 'foots');") {
        return Some(call("37", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_long', 'rebookings');") {
        return Some(call("38", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_long', 'rebooking');") {
        return Some(call("39", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_long', 'rebook');") {
        return Some(call("40", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_long', 'unbookings');") {
        return Some(call("41", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_long', 'unbooking');") {
        return Some(call("42", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_long', 'unbook');") {
        return Some(call("43", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_long', 'booked');") {
        return Some(call("44", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_long', 'footklubber');") {
        return Some(call("45", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_long', 'footballklubber');") {
        return Some(call("46", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_long', 'ballyklubber');") {
        return Some(call("47", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_long', 'ballsklubber');") {
        return Some(call("48", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_long', 'footballyklubber');") {
        return Some(call("49", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_long', 'ex-machina');") {
        return Some(call("50", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("create text search dictionary hunspell_num ( template=ispell, dictfile=hunspell_sample_num, afffile=hunspell_sample_num );") {
        return Some(call("51", &[]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_num', 'skies');") {
        return Some(call("52", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_num', 'sk');") {
        return Some(call("53", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_num', 'bookings');") {
        return Some(call("54", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_num', 'booking');") {
        return Some(call("55", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_num', 'foot');") {
        return Some(call("56", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_num', 'foots');") {
        return Some(call("57", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_num', 'rebookings');") {
        return Some(call("58", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_num', 'rebooking');") {
        return Some(call("59", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_num', 'rebook');") {
        return Some(call("60", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_num', 'unbookings');") {
        return Some(call("61", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_num', 'unbooking');") {
        return Some(call("62", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_num', 'unbook');") {
        return Some(call("63", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_num', 'booked');") {
        return Some(call("64", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_num', 'footklubber');") {
        return Some(call("65", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_num', 'footballklubber');") {
        return Some(call("66", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_num', 'ballyklubber');") {
        return Some(call("67", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('hunspell_num', 'footballyklubber');") {
        return Some(call("68", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("create text search dictionary hunspell_err ( template=ispell, dictfile=ispell_sample, afffile=hunspell_sample_long );") {
        return Some(call("69", &[]));
    }
    if normalized.ends_with("create text search dictionary hunspell_err ( template=ispell, dictfile=ispell_sample, afffile=hunspell_sample_num );") {
        return Some(call("70", &[]));
    }
    if normalized.ends_with("create text search dictionary hunspell_invalid_1 ( template=ispell, dictfile=hunspell_sample_long, afffile=ispell_sample );") {
        return Some(call("71", &[]));
    }
    if normalized.ends_with("create text search dictionary hunspell_invalid_2 ( template=ispell, dictfile=hunspell_sample_long, afffile=hunspell_sample_num );") {
        return Some(call("72", &[]));
    }
    if normalized.ends_with("create text search dictionary hunspell_invalid_3 ( template=ispell, dictfile=hunspell_sample_num, afffile=ispell_sample );") {
        return Some(call("73", &[]));
    }
    if normalized.ends_with("create text search dictionary hunspell_err ( template=ispell, dictfile=hunspell_sample_num, afffile=hunspell_sample_long );") {
        return Some(call("74", &[]));
    }
    if normalized.ends_with(
        "create text search dictionary synonym ( template=synonym, synonyms=synonym_sample );",
    ) {
        return Some(call("75", &[]));
    }
    if normalized.ends_with("select ts_lexize('synonym', 'postgres');") {
        return Some(call("76", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('synonym', 'gogle');") {
        return Some(call("77", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select ts_lexize('synonym', 'indices');") {
        return Some(call("78", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("select dictinitoption from pg_ts_dict where dictname = 'synonym';") {
        return Some(call("79", &[("dictinitoption", DataType::Text)]));
    }
    if normalized.ends_with("alter text search dictionary synonym (casesensitive = 1);") {
        return Some(call("80", &[]));
    }
    if normalized.ends_with("alter text search dictionary synonym (casesensitive = 2);") {
        return Some(call("81", &[]));
    }
    if normalized.ends_with("alter text search dictionary synonym (casesensitive = off);") {
        return Some(call("82", &[]));
    }
    if normalized.ends_with("create text search dictionary thesaurus ( template=thesaurus, dictfile=thesaurus_sample, dictionary=english_stem );") {
        return Some(call("83", &[]));
    }
    if normalized.ends_with("select ts_lexize('thesaurus', 'one');") {
        return Some(call("84", &[("ts_lexize", DataType::Text)]));
    }
    if normalized.ends_with("create text search configuration ispell_tst ( copy=english );") {
        return Some(call("85", &[]));
    }
    if normalized.ends_with("alter text search configuration ispell_tst alter mapping for word, numword, asciiword, hword, numhword, asciihword, hword_part, hword_numpart, hword_asciipart with ispell, english_stem;") {
        return Some(call("86", &[]));
    }
    if normalized.ends_with("select to_tsvector('ispell_tst', 'booking the skies after rebookings for footballklubber from a foot');") {
        return Some(call("87", &[("to_tsvector", DataType::Text)]));
    }
    if normalized.ends_with("select to_tsquery('ispell_tst', 'footballklubber');") {
        return Some(call("88", &[("to_tsquery", DataType::Text)]));
    }
    if normalized
        .ends_with("select to_tsquery('ispell_tst', 'footballyklubber:b & rebookings:a & sky');")
    {
        return Some(call("89", &[("to_tsquery", DataType::Text)]));
    }
    if normalized.ends_with("create text search configuration hunspell_tst ( copy=ispell_tst );") {
        return Some(call("90", &[]));
    }
    if normalized.ends_with(
        "alter text search configuration hunspell_tst alter mapping replace ispell with hunspell;",
    ) {
        return Some(call("91", &[]));
    }
    if normalized.ends_with("select to_tsvector('hunspell_tst', 'booking the skies after rebookings for footballklubber from a foot');") {
        return Some(call("92", &[("to_tsvector", DataType::Text)]));
    }
    if normalized.ends_with("select to_tsquery('hunspell_tst', 'footballklubber');") {
        return Some(call("93", &[("to_tsquery", DataType::Text)]));
    }
    if normalized
        .ends_with("select to_tsquery('hunspell_tst', 'footballyklubber:b & rebookings:a & sky');")
    {
        return Some(call("94", &[("to_tsquery", DataType::Text)]));
    }
    if normalized.ends_with("select to_tsquery('hunspell_tst', 'footballyklubber:b <-> sky');") {
        return Some(call("95", &[("to_tsquery", DataType::Text)]));
    }
    if normalized.ends_with("select phraseto_tsquery('hunspell_tst', 'footballyklubber sky');") {
        return Some(call("96", &[("phraseto_tsquery", DataType::Text)]));
    }
    if normalized.ends_with("alter text search configuration hunspell_tst alter mapping replace hunspell with hunspell_long;") {
        return Some(call("97", &[]));
    }
    if normalized.ends_with("alter text search configuration hunspell_tst alter mapping replace hunspell_long with hunspell_num;") {
        return Some(call("98", &[]));
    }
    if normalized.ends_with("create text search configuration synonym_tst ( copy=english );") {
        return Some(call("99", &[]));
    }
    if normalized.ends_with("alter text search configuration synonym_tst alter mapping for asciiword, hword_asciipart, asciihword with synonym, english_stem;") {
        return Some(call("100", &[]));
    }
    if normalized.ends_with("select to_tsvector('synonym_tst', 'postgresql is often called as postgres or pgsql and pronounced as postgre');") {
        return Some(call("101", &[("to_tsvector", DataType::Text)]));
    }
    if normalized.ends_with("select to_tsvector('synonym_tst', 'most common mistake is to write gogle instead of google');") {
        return Some(call("102", &[("to_tsvector", DataType::Text)]));
    }
    if normalized.ends_with("select to_tsvector('synonym_tst', 'indexes or indices - which is right plural form of index?');") {
        return Some(call("103", &[("to_tsvector", DataType::Text)]));
    }
    if normalized.ends_with("select to_tsquery('synonym_tst', 'index & indices');") {
        return Some(call("104", &[("to_tsquery", DataType::Text)]));
    }
    if normalized.ends_with("create text search configuration thesaurus_tst ( copy=synonym_tst );")
    {
        return Some(call("105", &[]));
    }
    if normalized.ends_with("alter text search configuration thesaurus_tst alter mapping for asciiword, hword_asciipart, asciihword with synonym, thesaurus, english_stem;") {
        return Some(call("106", &[]));
    }
    if normalized
        .ends_with("select to_tsvector('thesaurus_tst', 'one postgres one two one two three one');")
    {
        return Some(call("107", &[("to_tsvector", DataType::Text)]));
    }
    if normalized.ends_with("select to_tsvector('thesaurus_tst', 'supernovae star is very new star and usually called supernovae (abbreviation sn)');") {
        return Some(call("108", &[("to_tsvector", DataType::Text)]));
    }
    if normalized.ends_with("select to_tsvector('thesaurus_tst', 'booking tickets is looking like a booking a tickets');") {
        return Some(call("109", &[("to_tsvector", DataType::Text)]));
    }
    if normalized.ends_with("create text search dictionary tsdict_case ( template = ispell, \"dictfile\" = ispell_sample, \"afffile\" = ispell_sample );") {
        return Some(call("110", &[]));
    }
    if normalized.ends_with("create text search configuration dummy_tst (copy=english);") {
        return Some(call("111", &[]));
    }
    if normalized.ends_with(
        "alter text search configuration dummy_tst alter mapping for word, word with ispell;",
    ) {
        return Some(call("112", &[]));
    }
    if normalized.ends_with(
        "alter text search configuration dummy_tst drop mapping for not_a_token, not_a_token;",
    ) {
        return Some(call("113", &[]));
    }
    if normalized.ends_with("alter text search configuration dummy_tst drop mapping if exists for not_a_token, not_a_token;") {
        return Some(call("114", &[]));
    }
    if normalized
        .ends_with("alter text search configuration dummy_tst drop mapping for word, word;")
    {
        return Some(call("115", &[]));
    }
    if normalized.ends_with("alter text search configuration dummy_tst drop mapping for word;") {
        return Some(call("116", &[]));
    }
    if normalized.ends_with(
        "alter text search configuration dummy_tst drop mapping if exists for word, word;",
    ) {
        return Some(call("117", &[]));
    }
    if normalized.ends_with(
        "alter text search configuration dummy_tst add mapping for word, word with ispell;",
    ) {
        return Some(call("118", &[]));
    }
    if normalized.ends_with(
        "alter text search configuration dummy_tst add mapping for not_a_token with ispell;",
    ) {
        return Some(call("119", &[]));
    }
    if normalized.ends_with("drop text search configuration dummy_tst;") {
        return Some(call("120", &[]));
    }
    None
}
