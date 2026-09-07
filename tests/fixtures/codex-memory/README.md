# Memory merge scenario matrix

These are synthetic examples, not copies of user memory. `scenarios.json` is
executed by the Rust unit test in both endpoint orders. `merged: null` means
that the current text algorithm must request conflict resolution.

| Scenario | Current expected behavior |
| --- | --- |
| Same rule changes to restart versus reload | Conflict |
| Same retention rule changes to 3 versus 7 copies | Conflict |
| Independent conversations add to separate existing sections | Preserve both additions |
| Independent conversations append distinct sections at the same position | Conflict; no inferred ordering |
| Unrelated replacements of the same original line | Conflict, despite low similarity |
| Identical additions | Keep once |
| One side deletes a rule and the other edits it | Conflict |
| Must restart and must not restart are added on disjoint lines | **Text merge succeeds, semantic review required** |

The last case deliberately characterizes a known limitation. Passing this test
is not evidence that contradictory instructions are safe. No semantic inference
or similarity threshold is implemented. A future semantic or entry-aware policy
should replace that expected behavior explicitly, rather than silently treating
textual non-overlap as semantic independence.

Adapter tests additionally check that similar content in separate conversation
files remains additive, while unrelated content at the same path without a
shared baseline remains a conflict. Filenames identify synchronization objects;
they do not prove the conclusions in those files are mutually compatible.

Run the scenario matrix with:

```sh
cargo test realistic_memory_scenarios
cargo test separate_conversation_files
cargo test same_path_without_baseline
```
