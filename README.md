# Mini Event Backtester

A small C++20 event-driven backtester built to study deterministic market replay,
price-time priority, strategy latency, transaction costs, and basic production
safeguards. It replays the included synthetic order events, derives top-of-book
state, and lets an inventory-aware fair-price strategy submit marketable limit
orders.

## Features

- generic stable time-ordered event pool
- FIFO order book with add, cancel, and modify handling
- trained imbalance-based fair-price strategy
- inventory and order-size risk checks
- measured and configured latency components
- fixed and percentage-of-notional transaction fees
- stale-prediction rejection
- CSV scenario comparison with PnL and equity-range metrics

## Build

From the repository root with GCC or Clang:

```text
g++ -std=c++20 -O2 -Wall -Wextra -pedantic src/main.cpp -o mini-event-backtester
```

## Run

Use the included simulation stream:

```text
./mini-event-backtester
```

On Windows PowerShell, run `./mini-event-backtester.exe`. A different event file
can be supplied as the first argument:

```text
./mini-event-backtester path/to/events.txt
```

The program writes a CSV header and one result row per latency/cost scenario to
standard output. Lifecycle and error messages go to standard error.

## Input Format

Each whitespace-separated line is:

```text
timestamp action order_id owner_id side quantity price
```

`action` is `ADD`, `CANCEL`, or `MODIFY`; `side` is `BUY` or `SELL`.

## Repository Structure

- `src/main.cpp` - engine, book, strategy, risk checks, and CLI
- `lesson07_simulation_events.txt` - out-of-sample simulation stream
- `lesson07_training_events.txt` - historical training event stream
- `lesson07_training_snapshots.csv` - fitted-model training snapshots
- `docs/architecture.md` - component and event-flow design
- `docs/backtest-assumptions.md` - simulation assumptions and caveats
- `docs/reliability.md` - configuration, logging, and safety behavior
- `docs/results.md` - measured before-and-after scenario results
- `docs/presentation.md` - short project walkthrough

## Known Limitations

- one instrument and one strategy are simulated at a time
- historical replay cannot model how strategy orders would change other traders
- missed marketable orders disappear instead of resting in the book
- average fill price uses integer ticks
- the educational book uses linear scans for cancellation within one price level
- there is no live exchange connectivity or persistence

## Main architecture

## Event Flow

1. `main` loads and validates the historical event file.
2. `Backtest` inserts events into `TimeOrderedEventPool<MarketEvent>`.
3. Equal-time events are applied to `Book` in original insertion order.
4. `Book` exposes an aggregate, read-only `TopOfBook` view.
5. `FairPriceStrategy` returns at most one `OrderCommand` for a snapshot.
6. `RiskManager` checks the command before `Backtest` schedules its latency.
7. Due commands execute against the then-current book or are counted as stale or missed.
8. `Portfolio` applies fills and fees; `BacktestResult` records metrics and trades.

## Components

- `TimeOrderedEventPool` owns scheduled market events and provides deterministic order.
- `Book` owns resting orders and preserves best-price/FIFO priority.
- `Strategy` is a read-only decision interface; it cannot update execution state.
- `RiskManager` owns pre-trade safety policy.
- `Portfolio` owns cash, position, fee accounting, and marked equity.
- `Backtest` owns orchestration state and a polymorphic strategy.
- `Logger` keeps operational output separate from the CSV result stream.

The implementation remains in one source file to match the course's complete
single-file reference, while the class boundaries allow later extraction into
headers and implementation files.

## Backtest asumptions

## Market Replay

- Input events are for one instrument and timestamps are non-negative integer ticks.
- Events may arrive out of file order; the event pool orders them stably by timestamp.
- Adds and modifications require positive integer price and quantity.
- An unknown cancel is harmless because historical streams may cancel an already-filled order.
- Price priority is best price first and time priority is FIFO within one price level.

## Strategy and Execution

- Fair price is midpoint plus fitted bias and order-book imbalance weight.
- The strategy submits at most one unit per market snapshot.
- Pending inventory is included in risk and inventory-aware edge calculations.
- Commands are marketable limit orders; they execute fully or miss without changing the book.
- Strategy orders do not affect how future historical participants behave.
- Inventory is marked at the latest midpoint, falling back to the last valid top of book.

## Costs and Timing

- Fixed fees are charged per filled unit.
- The optional percentage fee is charged on absolute trade notional. The one-basis-point
  scenario demonstrates that the original fixed-fee result was too optimistic because
  every completed trade pays an additional cost on the same event stream.
- Communication, prediction generation, prediction transfer, and measured strategy time
  are separate latency components.
- A configured age limit can reject stale predictions before execution.

## Known Limitations

- Historical replay cannot estimate market reaction to the strategy's visible orders.
- There is no passive queue-position model, slippage curve, partial strategy fill, or impact model.
- The input contains synthetic data, so profitable output is not evidence of live profitability.
- Cancellation scans a FIFO price level linearly; this favors clarity over the object-pool
  optimization discussed in the course.