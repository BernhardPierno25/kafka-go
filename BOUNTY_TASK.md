# Bounty Fix

**Reward:** $10.00
**Issue:** https://github.com/BernhardPierno25/kafka-go/issues/1
**Repo:** https://github.com/BernhardPierno25/kafka-go

---

## Issue

🎯 Prevent WriteMessages from Returning Context Timeout After Messages Are Delivered

## 📝 Description
In `kafka.Writer`, calling `WriteMessages(ctx, ...)` with a context deadline can occasionally return `context.DeadlineExceeded` (or `context.Canceled`) even after the messages have been successfully written and acknowledged by the Kafka broker.

This is caused by a race condition in the completion handling of `WriteMessages`:
When a batch is written to the broker and the response is received, the batch's completion channel is signaled. If the context expires at or near the same instant the response is processed, Go's `select` statement between `<-ctx.Done()` and the response channel selects `ctx.Done()` non-deterministically. As a result, `WriteMessages` returns `ctx.Err()` instead of `nil`.

This false-negative error leads callers to assume message delivery failed, often triggering unnecessary retries (causing message duplication) or raising false alarms in upstream error logging and transaction handling.

---

## 🎯 Acceptance Criteria
- [ ] If message delivery is successfully acknowledged by the Kafka broker before or simultaneously with context cancellation/deadline expiration, `WriteMessages` must return `nil` (or the actual broker result) rather than `ctx.Err()`.
- [ ] Implement a two-phase check or non-blocking channel probe upon context cancellation to resolve race conditions between `ctx.Done()` and batch completion.
- [ ] Context cancellation/deadline expiration must still be respected and returned promptly if the broker has not yet acknowledged the batch.
- [ ] No goroutine leaks, channel deadlocks, or uncollected batch promises under heavy timeout scenarios.
- [ ] Existing behavior for partial writes, retries, and genuine timeouts must remain backward-compatible and functional.

---

## 🛠️ Technical Specifications & Context
- **Target Files:**
  - `writer.go` (specifically `Writer.WriteMessages`, `Writer.produce`, or internal batch wait logic)
  - `writer_test.go`
- **Implementation Details:**
  1. In `writer.go`, locate the synchronization point where `WriteMessages` waits for batch completion (e.g., waiting on a response channel / promise).
  2. When the `case <-ctx.Done():` branch triggers, perform a non-blocking check on the result channel before returning `ctx.Err()`:
     ```go
     select {
     case res := <-resChan:
         return res
     case <-ctx.Done():
         // Check if the result was already delivered concurrently
         select {
         case res := <-resChan:
             return res
         default:
             return ctx.Err()
         }
     }
     ```
  3. If batch promises/results are used across partitions, ensure all associated partition results are evaluated so that confirmed writes take precedence over context errors once the write cycle completes.

---

## 🧪 Verification & Testing
- **Unit Testing (`writer_test.go`):**
  - Add a test `TestWriterContextDeadlineRace` using a mock transport / `RoundTripper` that completes message writes concurrently with context cancellation.
  - Run the test under `go test -race -count=100 ./...` to guarantee that the `select` race condition is eliminated and no data races are introduced.
- **Integration Testing:**
  - Run a test producing messages against a local Kafka broker with tight context deadlines, asserting that when messages exist in the topic, `WriteMessages` did not report a false `context.DeadlineExceeded`.

---

[![Opire Bounty](https://img.shields.io/badge/bounty-Opire-emerald?style=for-the-badge)](https://github.com/BernhardPierno25/kafka-go)
<br/>
<hr/>

<details><summary>This repo is using Opire - what does it mean? 👇</summary><br/>💵 Everyone can add rewards for this issue commenting <code>/reward 100</code> (replace <code>100</code> with the amount).<br/>🕵️‍♂️ If someone starts working on this issue to earn the rewards, they can comment <code>/try</code> to let everyone know!<br/>🙌 And when they open the PR, they can comment <code>/claim #1</code> either in the PR description or in a PR's comment.<br/><br/>🪙 Also, everyone can tip any user commenting <code>/tip 20 @BernhardPierno25</code> (replace <code>20</code> with the amount, and <code>@BernhardPierno25</code> with the user to tip).<br/><br/>📖 If you want to learn more, check out our <a href="https://docs.opire.dev">documentation</a>.</details>

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
- git commit -m "fix: 🎯 Prevent WriteMessages from Returning Context Timeout After Messages A"
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
