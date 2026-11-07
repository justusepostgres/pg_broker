use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::Cursor;
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Data(serde_json::Value);
impl std::fmt::Display for Data {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "compatibility fixture")
    }
}
use std::collections::{BTreeMap, BTreeSet};
openraft_old::declare_raft_types!(pub Old: D = Data, R = ());
openraft_next::declare_raft_types!(pub Next: D = Data, R = ());
fn roundtrip<A: Serialize + DeserializeOwned, B: Serialize + DeserializeOwned>(
    label: &str,
    old: A,
) {
    let before = serde_json::to_value(old).unwrap();
    let next: B = serde_json::from_value(before.clone()).unwrap();
    let after = serde_json::to_value(next).unwrap();
    assert_eq!(before, after, "{label}");
    let _: A = serde_json::from_value(after).unwrap();
    println!("PASS {label}: {before}");
}
fn main() {
    use openraft_next::type_config::alias::{EntryOf, LogIdOf, StoredMembershipOf, VoteOf};
    let id = openraft_old::LogId::new(openraft_old::CommittedLeaderId::new(7, 2), 42);
    roundtrip::<_, LogIdOf<Next>>("log ID", id);
    roundtrip::<_, VoteOf<Next>>("committed vote", openraft_old::Vote::new_committed(7, 2));
    roundtrip::<_, VoteOf<Next>>("uncommitted vote", openraft_old::Vote::new(8, 3));
    let membership = openraft_old::Membership::new(
        vec![BTreeSet::from([1, 2, 3])],
        BTreeMap::from([
            (1, openraft_old::BasicNode::new("node1")),
            (2, openraft_old::BasicNode::new("node2")),
            (3, openraft_old::BasicNode::new("node3")),
        ]),
    );
    roundtrip::<_, StoredMembershipOf<Next>>(
        "stored membership",
        openraft_old::StoredMembership::new(Some(id), membership.clone()),
    );
    for (label, payload) in [
        ("blank", openraft_old::EntryPayload::Blank),
        (
            "normal",
            openraft_old::EntryPayload::Normal(Data(serde_json::json!([1, 2, 3]))),
        ),
        (
            "append command envelope",
            openraft_old::EntryPayload::Normal(Data(serde_json::json!({
                "op": "append", "expected_offset": 0, "appended_at_ms": 1700000000123_u64,
                "data": include_str!("../../../../crates/cluster/tests/fixtures/append-batch.hex").trim()
            }))),
        ),
        (
            "retention command envelope",
            openraft_old::EntryPayload::Normal(Data(serde_json::json!({
                "op": "retain", "expected_earliest": 0, "new_earliest": 4096
            }))),
        ),
        (
            "membership",
            openraft_old::EntryPayload::Membership(membership),
        ),
    ] {
        roundtrip::<_, EntryOf<Next>>(
            label,
            openraft_old::Entry::<Old> {
                log_id: id,
                payload,
            },
        );
    }
    println!("Scope: selected JSON fixtures only; not a storage or consensus migration test.");
}

#[cfg(test)]
mod stream_tests;

#[test]
fn selected_json_fixtures_round_trip() {
    main();
}

#[cfg(test)]
mod paged_stream;

#[cfg(test)]
mod postgres_stream;
