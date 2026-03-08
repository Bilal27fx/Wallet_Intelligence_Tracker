# WIT AI Team Report — Score Engine — scoring model, FIFO accuracy, thresholds

### Final Report for Score Engine Improvements

## Summary
The team identified several key issues impacting the Score Engine of WIT V1, focusing on dynamic thresholding, FIFO transaction handling, and wallet analysis coverage. Although the proposals are well-founded, additional analysis and validation are needed for full implementation. The next step involves confirming these proposals, especially those flagged as unverified or requiring more analysis.

## Prioritized Improvements

| # | Title                                   | Impact | Effort | Confidence | Status                |
|---|-----------------------------------------|--------|--------|------------|-----------------------|
| 1 | Dynamic Thresholds in Score Engine      | High   | Medium | Medium     | NEEDS MORE ANALYSIS   |
| 2 | Enhanced FIFO Transaction Handling      | Medium | Low    | High       | NEEDS MORE ANALYSIS   |
| 3 | Stratified Sampling for Wallet Coverage | Medium | Medium | Medium     | NEEDS MORE ANALYSIS   |
| 4 | Dynamic Stablecoin List                 | Low    | Low    | High       | READY TO IMPLEMENT    |
| 5 | API-driven ETH Price Updates            | Low    | Medium | Medium     | NEEDS MORE ANALYSIS   |

## Implementation Plan (READY items only)

- **Dynamic Stablecoin List**
  - **File**: None directly impacted (Configuration update)
  - **Change Summary**: Regularly refresh list of stablecoins using a reliable API such as CoinGecko or CoinMarketCap.
  - **Config.py Keys to Add**:
    ```python
    "STABLECOIN_SOURCES": {"coingecko": {"url": "https://api.coingecko.com/api/v3/simple/supported_vs_currencies"}},
    ```

## Database Changes
- No database migrations proposed that are SQLite-incompatible. All changes are valid for the current SQLite setup.

## Decisions Required from Bilal
1. **Dynamic Threshold Proposal**: Approve further analysis on implementing data-driven dynamic thresholds in the score engine.
2. **FIFO Handling**: Validate the revised approach to flag transactions instead of discarding them.
3. **Stratified Sampling**: Confirm if additional development on stratified sampling for wallet coverage is necessary.
4. **API-driven Data Updates**: Approve exploration of API-driven methods for real-time data updates (stablecoin lists, ETH prices).

## What NOT to Change
- **Manual Market Price Fallback**: Decided to keep existing method until a robust API alternative is proven effective.
- **Hardcoded Price Caps**: Current logic remains as no strong evidence suggested optimal new caps.

## Team Confidence Assessment
- All team members effectively utilized their tools, but the confidence in dynamic thresholding needs reinforcement through data analysis verification.
- FIFO handling recommendations showed solid reasoning with high confidence in evidence.
- Proposals such as API-driven updates lacked mature empirical backing, hence flagged for further exploration.

This report is built on the premise that foundational changes, once validated, present significant impact on analytical precision and robustness. Your approval and decision requests are crucial for moving forward.