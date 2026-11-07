//! Experimental building blocks for independent PostgreSQL-backed replicas.
//! Local persistence does not implement consensus, record replication, or leader fencing.
pub mod application;
pub mod journal;
pub mod reader;
pub mod snapshot;
pub mod snapshot_file;
pub mod snapshot_stream;
pub mod storage;
pub mod votes;
