# Agent Summary — Bounty #2 (`Writer.WriteMessages` context timeout)

UNCERTAIN: The target repository does not contain the code the issue describes. `BernhardPierno25/kafka-go` is an empty placeholder repo — its entire contents, across every branch and including `upstream/main`, are `README.md` (one line: `# kafka-go`) and a hello-world `main.go`. There is no `writer.go`, no `writer_test.go`, no `go.mod`, and no `Writer` type anywhere. No code changes were made.

## What the issue asked for

Make `Writer.WriteMessages(ctx, msgs...)` prefer a resolved broker-delivery result over `ctx.Done()` when both are ready, via a double-checked `select` on the batch completion channel, plus unit/integration/race tests in `writer_test.go`.

## Orientation performed

- `README.md` — a single line, `# kafka-go`. No docs, no package description.
- Full file listing (excluding `.git`): `BOUNTY_TASK.md`, `README.md`, `main.go`. That is the whole repo.
- `main.go` is 7 lines:
  ```go
  package main

  import "fmt"

  func main() {
  	fmt.Println("Hello, Bounty Hunter!")
  }
  ```
- Grepped the repo for every key term from the issue (`WriteMessages`, `writeBatch`, `Writer`, `promise`, `ctx.Done`) across `*.go` and `*.md` — **zero matches** outside `BOUNTY_TASK.md` itself.
- `git log --all --pretty=format: --name-only | sort -u` → only `README.md` and `main.go` have ever existed in history. Two commits total: `f8a7d72 Initial commit`, `adfbe64 Add starter code file`.
- Fetched `upstream` (`github.com/BernhardPierno25/kafka-go`) to rule out a stale local clone. `git ls-tree -r --name-only upstream/main` → `README.md`, `main.go`. Upstream history is identical to local (`adfbe64`, `f8a7d72`). The placeholder is the real upstream state, not a sync artifact.

## Tests

None exist. `find . -name "*_test.go"` → 0 results. There is no `go.mod`, so there is no module, no package `kafka`, and no test suite to read for expected behavior. Step 2 of the process (read the tests to learn what a passing fix looks like) has no input available.

## Why this is a stop, not a fix

The issue is written against **`segmentio/kafka-go`** — a real library whose `Writer` has a batching pipeline, `writeBatch`, and per-batch completion channels. This repo is not a fork or copy of it; it shares only the name. Delivering the requested fix here would mean authoring the `Writer` type, its batching/partitioning pipeline, broker transport, and completion-channel machinery from scratch before the ~10-line `select` change could even be expressed. That is inventing a library, not a minimal one-logical-change fix, and it would blow well past the ~5-file limit in the stop conditions. It also could not be verified: there is no module, no tests, and no Kafka broker — `go` is not even installed on this machine (`go: NOT FOUND on PATH` via both bash and PowerShell), so `go test -race -run TestWriter ./...` from the verification section cannot run.

Matching stop conditions:
- **Cannot determine what the correct behavior should be** — the code under discussion is absent, so there is nothing to diff against and no existing behavior to stay backward-compatible with (an explicit acceptance criterion).
- **The fix requires changing more than ~5 files** — from-scratch implementation, not an edit.

## For the reviewer

- The pre-existing working-tree diff on `main.go` is **not mine**. It was present in the snapshot before I started and is purely a line-ending change (LF → CRLF); content is byte-identical otherwise. I left it untouched rather than commit or revert it.
- Nothing was committed. The only file I created is this `AGENT_SUMMARY.md`.
- If the intent was to fix the upstream `segmentio/kafka-go`, this bounty needs to be re-pointed at a repo that actually contains `writer.go`. The fix itself is small and well-specified — the recommended double-checked `select` in the issue body is the correct shape — so it is readily actionable *once aimed at real source*.
- Separately: the `origin` remote URL embeds a live GitHub OAuth token (`gho_…`) in plaintext. Worth rotating.
