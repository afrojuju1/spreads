# Options Alpha Product Research

As of: Wednesday, April 15, 2026

Related:

- [Alpaca Options Automation System Architecture](../planning/2026-04-15_alpaca_options_automation_system_architecture.md)

## Goal

Capture the parts of the Options Alpha product model that are worth emulating.

This is product research, not our system design.

## Thesis

Options Alpha is best understood as a retail options automation product built around a small set of abstractions that make systematic trading usable without code.

The key idea is not "options scanner plus broker integration." The key idea is a full loop:

- find opportunities
- express a repeatable strategy as rules
- run the rules inside isolated bots
- automate entries and exits
- paper trade and backtest before going live
- inspect logs to understand every decision

## Core Product Primitives

### Bot

A bot is an isolated strategy container with its own capital budget, position limits, and automations.

This is one of the strongest product choices because it gives users a clean mental model for running multiple systems at once.

### Automation

An automation is a workflow that decides when to act, what to trade, and how to manage open positions.

Important characteristics:

- can run on schedules
- can run on events
- can be reused across bots
- can be tested before activation

### Recipe / Decision Blocks

Options Alpha exposes strategy logic as composable decisions and actions rather than user code.

Examples from the public product surface:

- technical indicator checks
- opportunity filters
- earnings-based decisions
- position-management decisions
- grouped and/or logic

### Template

Templates let users start from a working strategy and customize it rather than build everything from scratch.

This lowers time-to-value and is probably required for retail adoption.

### Backtest

The backtester is not just a research tool. It is part of the product funnel.

The important design move is that a tested strategy can be promoted into a bot, which closes the gap between research and live automation.

## Product Behaviors Worth Copying

### 1. Isolated strategy containers

Users think in terms of "this bot trades this playbook with this amount of capital," not in terms of global account scripts.

### 2. Clear automation lifecycle

The platform gives users one place to define:

- entry logic
- exit logic
- risk limits
- scheduling
- position management

### 3. First-class decision logs

One of the most important features is transparency. The system records why a bot did or did not act.

This is critical for trust.

### 4. Smart order management

Options Alpha presents pricing logic as part of the product, not as a low-level broker detail.

The public surface emphasizes:

- dynamic repricing
- timed retry intervals
- bid/ask spread awareness
- better automated entry and exit handling

### 5. Paper and live on the same mental model

The user should not have to learn a separate product for testing versus live automation.

### 6. Backtest to automation promotion

This is a strong product loop because it turns research into deployable behavior rather than leaving it as a report.

## Likely Internal Product Requirements

An Options Alpha-style system likely needs the following internal capabilities even if the user mostly sees a no-code surface:

- a constrained strategy DSL
- a deterministic bot runtime
- point-in-time market snapshots
- strategy-position modeling for multi-leg strategies
- a smart-pricing service
- a paper execution engine
- an auditable decision log
- a replayable event model

## What Matters Less For Our First Version

These are valuable product layers, but they are not the core of the system:

- community sharing
- rich template marketplace features
- full no-code breadth on day one
- broad social/copy-trading mechanics
- dozens of strategy families immediately

## Implications For Our Design

If we want something meaningfully similar, we should copy the operating model more than the surface aesthetics.

That means prioritizing:

- bots as isolated capital buckets
- reusable automations
- constrained decision recipes
- smart pricing for entries and exits
- strong paper trading and replay
- exhaustive logs for operator trust

## Sources

- [Option Alpha](https://optionalpha.com/)
- [Option Alpha Bots](https://optionalpha.com/bots)
- [Option Alpha Backtester](https://optionalpha.com/backtester)
