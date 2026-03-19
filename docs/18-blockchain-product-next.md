# Blockchain-Focused Next Steps (Post-Roadmap)

After completing the current GraniteDB documents/roadmap, proceed with blockchain-oriented features and product hardening:

- Deterministic iteration + snapshot isolation (state proofs, deterministic block execution).
- Fast checkpoints / state sync (copy-on-write style snapshots, or ingest of prebuilt SST sets).
- Compaction control tuned for write-heavy + read-heavy mixed workloads.
- Operational/tooling layer: config, metrics, corruption detection, repair, backup/restore flows.

