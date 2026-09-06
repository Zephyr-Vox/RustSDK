use std::path::PathBuf;

use serde::Deserialize;
use zephyrvox_testkit::{FixtureError, FixtureLoader};

#[test]
fn fixture_loader_reads_json_below_its_root() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let loader = FixtureLoader::new(root);
    let fixture: ExampleFixture = loader.read_json("example.json").expect("fixture");
    assert_eq!(fixture.name, "phase1");
    assert_eq!(fixture.version, 1);
    assert!(
        loader
            .read_string("example.json")
            .expect("text")
            .contains("phase1")
    );
}

#[test]
fn fixture_loader_rejects_path_escape() {
    let loader = FixtureLoader::new("tests/fixtures");
    assert!(matches!(
        loader.read_bytes("../example.json"),
        Err(FixtureError::InvalidName(_))
    ));
    assert!(matches!(
        loader.read_bytes("/tmp/example.json"),
        Err(FixtureError::InvalidName(_))
    ));
}

#[derive(Debug, Deserialize)]
struct ExampleFixture {
    name: String,
    version: u8,
}
