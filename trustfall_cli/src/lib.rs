use std::{collections::BTreeMap, fmt::Debug, sync::Arc};

use serde::{Deserialize, Serialize};
use trustfall_core::{
    frontend::error::FrontendError,
    graphql_query::error::ParseError,
    interpreter::trace::Trace,
    ir::{FieldValue, IRQuery},
};

#[macro_use]
extern crate maplit;

pub mod filesystem_interpreter;
pub mod nullables_interpreter;
pub mod numbers_interpreter;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestGraphQLQuery {
    pub schema_name: String,

    pub query: String,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub arguments: BTreeMap<String, FieldValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestParsedGraphQLQuery {
    pub schema_name: String,

    // pub query: Query,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub arguments: BTreeMap<String, FieldValue>,
}

pub type TestParsedGraphQLQueryResult = Result<TestParsedGraphQLQuery, ParseError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestIRQuery {
    pub schema_name: String,

    pub ir_query: IRQuery,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub arguments: BTreeMap<String, FieldValue>,
}

pub type TestIRQueryResult = Result<TestIRQuery, FrontendError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound = "DataToken: Serialize, for<'de2> DataToken: Deserialize<'de2>")]
pub struct TestInterpreterOutputTrace<DataToken>
where
    DataToken: Clone + Debug + PartialEq + Eq + Serialize,
    for<'de2> DataToken: Deserialize<'de2>,
{
    pub schema_name: String,

    pub trace: Trace<DataToken>,

    pub results: Vec<BTreeMap<Arc<str>, FieldValue>>,
}

#[cfg(test)]
mod tests {
    use std::{
        fmt::Debug,
        fs,
        path::{Path, PathBuf},
    };

    use serde::{Deserialize, Serialize};
    use trustfall_filetests_macros::parameterize;

    use crate::{
        filesystem_interpreter::FilesystemToken, numbers_interpreter::NumbersToken, TestIRQuery,
        TestIRQueryResult, TestInterpreterOutputTrace,
    };
    use trustfall_core::interpreter::replay::assert_interpreted_results;

    fn check_trace<Token>(expected_ir: TestIRQuery, test_data: TestInterpreterOutputTrace<Token>)
    where
        Token: Debug + Clone + PartialEq + Eq + Serialize,
        for<'de> Token: Deserialize<'de>,
    {
        // Ensure that the trace file's IR hasn't drifted away from the IR file of the same name.
        assert_eq!(expected_ir.ir_query, test_data.trace.ir_query);
        // assert_eq!(expected_ir.arguments, test_data.trace.arguments);

        assert_interpreted_results(&test_data.trace, &test_data.results, true);
    }

    fn check_filesystem_trace(expected_ir: TestIRQuery, input_data: &str) {
        match ron::from_str::<TestInterpreterOutputTrace<FilesystemToken>>(input_data) {
            Ok(test_data) => {
                assert_eq!(expected_ir.schema_name, "filesystem");
                assert_eq!(test_data.schema_name, "filesystem");
                check_trace(expected_ir, test_data);
            }
            Err(e) => {
                unreachable!("failed to parse trace file: {e}");
            }
        }
    }

    fn check_numbers_trace(expected_ir: TestIRQuery, input_data: &str) {
        match ron::from_str::<TestInterpreterOutputTrace<NumbersToken>>(input_data) {
            Ok(test_data) => {
                assert_eq!(expected_ir.schema_name, "numbers");
                assert_eq!(test_data.schema_name, "numbers");
                check_trace(expected_ir, test_data);
            }
            Err(e) => {
                unreachable!("failed to parse trace file: {e}");
            }
        }
    }

    #[parameterize("trustfall_core/src/resources/test_data/valid_queries")]
    fn parameterized_tester(base: &Path, stem: &str) {
        let mut input_path = PathBuf::from(base);
        input_path.push(format!("{stem}.trace.ron"));

        let input_data = fs::read_to_string(input_path).unwrap();

        let mut check_path = PathBuf::from(base);
        check_path.push(format!("{stem}.ir.ron"));
        let check_data = fs::read_to_string(check_path).unwrap();
        let expected_ir: TestIRQueryResult = ron::from_str(&check_data).unwrap();
        let expected_ir = expected_ir.unwrap();

        match expected_ir.schema_name.as_str() {
            "filesystem" => check_filesystem_trace(expected_ir, input_data.as_str()),
            "numbers" => check_numbers_trace(expected_ir, input_data.as_str()),
            _ => unreachable!("{}", expected_ir.schema_name),
        }
    }
}
