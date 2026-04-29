# Pipeline repair plan

## What the current code actually does
- The current `Aggregator` implementation is the market-data source: it fetches Binance REST history, opens WS streams, and publishes `DATA.CANDLE.*`.
- `AnalyzerMath` consumes candle topics, warms up from CSV, and publishes `ANALYSIS.FORECAST`.
- `MultiTFResolver` consumes the forecast and publishes `TRADE.CONTEXT`.
- `TradeCalc` consumes the context and publishes `TRADE.ORDER`.
- `RiskManager` consumes the order and publishes `EXECUTION.ORDER` or `RISK.REJECT`.
- There is no dedicated execution/dealer module yet.

## Where the mental model and the code diverged
- The original intent was closer to: analyzers -> aggregator -> strategist -> dealer.
- In the current code, `Aggregator` is not the future decision aggregator; it is a temporary market-data hub.
- The chain currently stops at risk approval; there is no module that actually executes the approved order.

## Clarified target meaning of "Aggregator"
- In the long term, the true aggregator should be the meta-layer that receives outputs from all analyzers.
- It should estimate the weight of each analyzer based on historical effectiveness, not only on the raw analyzer confidence.
- First it can be a deterministic stub; later it can be replaced by a neural model.
- That model should learn from realized outcomes: win/loss, expectancy, drawdown impact, and how often each analyzer led to good decisions.

## Naming decision to remove ambiguity
- Current data-ingestion module: rename `modules/aggregator.py` and `Aggregator` to `MarketDataCollector`.
- Future analyzer-weighting module: use `AnalysisAggregator` as the real aggregator name.
- Result: one name for feed collection, one name for analyzer weighting, no collision.

## Recommended direction
- Keep a single source of candles to avoid duplicate Binance traffic and rate-limit risk.
- Split the raw feed layer from the decision aggregation layer.
- Make the roles explicit instead of relying on overloaded names.
- Treat the last missing piece as a real execution/dealer stage.

## Proposed role map
- Market-data source: `MarketDataCollector`.
- Analysis: one or many analyzers that emit normalized analysis results.
- Meta-aggregation: `AnalysisAggregator`, the layer that consumes all analysis results and assigns weights.
- Strategy: `MultiTFResolver` or a later dedicated strategist that converts aggregated signal into a trade idea.
- Order sizing / trade math: `TradeCalc`.
- Pre-trade risk: `RiskManager`.
- Execution / dealer: new module that consumes `EXECUTION.ORDER` and talks to Binance.

## Repair steps
1. Confirm the canonical pipeline and the responsibility of each stage.
2. Rename the current ingestion module to `MarketDataCollector` everywhere.
3. Reserve `AnalysisAggregator` for the future analyzer-weighting layer and define its input payload.
4. Add or stub the execution/dealer module.
5. Rewire `Core` so the topic chain matches the role map.
6. Fix the test scripts so they subscribe to the correct topics and do not reuse the wrong socket types.
7. Verify the live flow end-to-end and then align the backtest flow with the same stage boundaries.

## Success criteria
- There is exactly one candle source.
- Analysis produces independent outputs that can be scored later.
- `AnalysisAggregator` can combine multiple analyzer outputs and assign weights.
- Strategy emits a single trade context.
- Risk approves or rejects before execution.
- Dealer is the only module allowed to place orders.
- A smoke test can observe the full topic path in order.

## Open decision
- Preserve the current centralized data-ingestion design, or move closer to the original literal idea where analyzers query APIs themselves.
- Recommended choice: centralized ingestion, with `MarketDataCollector` for feeds and `AnalysisAggregator` for weighted analyzer results.
