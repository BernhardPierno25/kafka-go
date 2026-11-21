# Bounty Fix

**Reward:** $10.00
**Issue:** https://github.com/BernhardPierno25/kafka-go/issues/2
**Repo:** https://github.com/BernhardPierno25/kafka-go

---

## Issue

🎯 Prevent `Writer.WriteMessages` from returning context timeout when messages have been successfully delivered

## 📝 Description
In `kafka-go`, calling `Writer.WriteMessages(ctx, msgs...)` can return `context.DeadlineExceeded` (or `context.Canceled`) even after the messages have been successfully transmitted to and acknowledged by the Kafka broker.

This issue stems from two primary mechanisms in Go's concurrency model and `Writer`'s batching pipeline:
1. **Pseudo-random `select` evaluation:** When awaiting message batch completion, if both `ctx.Done()` and the internal delivery promise/channel resolve at or near the same time, Go's `select` statement selects cases pseudo-randomly. As a result, `ctx.Done()` can be chosen despite the delivery promise holding a successful response (`nil` error).
2. **Context expiration during/immediately following broker ACK:** If the broker acknowledges the write just as the client-side timeout fires, `WriteMessages` exits with a context error without checking if the delivery actually succeeded.

When `WriteMessages` returns a timeout error for messages that were already committed to the partition, client applications typically execute retry logic, leading to duplicate records in Kafka topics.

## 🎯 Acceptance Criteria
- [ ] `Writer.WriteMessages` must prioritize a successful broker delivery acknowledgment over `ctx.Done()` when both are ready at evaluation time.
- [ ] If the internal batch completion channel has received a response (success or broker error), that response must be evaluated and returned before falling back to `ctx.Err()`.
- [ ] A non-blocking check on the completion/promise channel must be performed before returning `ctx.Err()`.
- [ ] In synchronous writes or batches where partial delivery cannot be verified after context expiration, ensure error types clearly distinguish between uncommitted timeouts and post-dispatch context cancellations.
- [ ] No goroutine leaks, channel deadlocks, or unhandled panics occur when context expires while batches are in flight.
- [ ] Existing `Writer` behavior (e.g., batching, compression, idempotent writes, required ACKs) remains backward-compatible.

## 🛠️ Technical Specifications & Context
- **Primary Locations:**
  - `writer.go`: Review `Writer.WriteMessages`, internal batch dispatching (`writeBatch`), and promise/completion channel handling.
  - `writer_test.go`: Unit and integration test suites for `Writer`.
- **Implementation Strategy:**
  - When waiting for the completion channel (`done` / `promise`), avoid a naive single `select`:
    ```go
    // Problematic pattern:
    select {
    case <-ctx.Done():
        return ctx.Err()
    case res := <-done:
        return res
    }
    ```
  - Implement a two-tier or double-checked resolution pattern to favor broker completion:
    ```go
    // Recommended pattern:
    select {
    case res := <-done:
        return res
    case <-ctx.Done():
        // Double-check if the delivery completed before or concurrently with ctx cancellation
        select {
        case res := <-done:
            return res
        default:
            return ctx.Err()
        }
    }
    ```
  - Ensure that batch completion handlers consistently close or populate completion channels before triggering any context-dependent callbacks.

## 🧪 Verification & Testing
- **Unit Test (Simulated Concurrent Completion):**
  - Add a test in `writer_test.go` where a mock transport/broker fulfills the message batch delivery and a pre-canceled or timed-out context is passed to `WriteMessages`. Verify that `WriteMessages` returns `nil` (or the broker result) instead of `context.Canceled`/`context.DeadlineExceeded`.
- **Integration Test (Race Condition Stress Test):**
  - Produce messages under high concurrency with tight context deadlines against a live Kafka cluster or local container.
  - Validate that when `WriteMessages` returns an error, the message offset was not committed, and when the offset was committed, `WriteMessages` returned `nil`.
- **Race Detection:**
  - Execute `go test -race -v -run TestWriter ./...` to verify there are no race conditions or channel leakage.

---

[![Opire Bounty](https://img.shields.io/badge/bounty-Opire-emerald?style=for-the-badge)](https://github.com/BernhardPierno25/kafka-go)
<br/>
<hr/>

<details><summary>This repo is using Opire - what does it mean? 👇</summary><br/>💵 Everyone can add rewards for this issue commenting <code>/reward 100</code> (replace <code>100</code> with the amount).<br/>🕵️‍♂️ If someone starts working on this issue to earn the rewards, they can comment <code>/try</code> to let everyone know!<br/>🙌 And when they open the PR, they can comment <code>/claim #2</code> either in the PR description or in a PR's comment.<br/><br/>🪙 Also, everyone can tip any user commenting <code>/tip 20 @BernhardPierno25</code> (replace <code>20</code> with the amount, and <code>@BernhardPierno25</code> with the user to tip).<br/><br/>📖 If you want to learn more, check out our <a href="https://docs.opire.dev">documentation</a>.</details>

---

## Process

### 1. Orient yourself first
Before touching any code:
- Read the README and any relevant docs
- Run: find . -type f | grep -v node_modules | grep -v .git | grep -v dist | head -60
- Grep for the key terms from the issue title to find the relevant files
- Read the files around the bug — understand what's currently happening

### 2. Find and read the tests
- Look for test files near the relevant code (*.test.ts, *.spec.js, __tests__, etc.)
- Read them — they tell you what behavior is expected
- Understand what a passing fix looks like before writing a line

### 3. Implement the fix
- Make the MINIMAL change that fixes the issue — nothing more
- Do not refactor, rename, reorganize, or clean up unrelated code
- Do not add dependencies unless the issue explicitly requires it
- Do not modify config files (package.json, tsconfig.json, etc.) unless that IS the fix
- One logical change, one commit

### 4. Verify
- Run the test suite: check package.json scripts for "test", "lint", "typecheck"
  Common commands: npm test, yarn test, pnpm test, pytest, go test ./..., cargo test
- If TypeScript is present: npx tsc --noEmit
- Fix any test failures your change caused — if a test was wrong before, update it

### 5. Commit and document
- git add -A
- git commit -m "fix: 🎯 Prevent 'Writer.WriteMessages' from returning context timeout when me"
- Write AGENT_SUMMARY.md with:
  - Root cause: what was wrong and why
  - Fix: what you changed and the reasoning
  - Verification: how to confirm it works
  - Concerns: anything a reviewer should know

---

## Stop conditions

If ANY of these are true, write "UNCERTAIN: [reason]" at the top of AGENT_SUMMARY.md, make NO code changes, and stop:
- The fix requires changing more than ~5 files
- You cannot determine what the correct behavior should be
- The issue requires a database migration, API key, or external service
- You find 3+ open PRs already attempting this fix (high competition)
- The repo is not in a language/framework you can reason about confidently

A clean UNCERTAIN is better than a bad PR.
