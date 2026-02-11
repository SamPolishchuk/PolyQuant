average.py takes all markets in trades_details.csv, does percentile comparisons between general public and insider, scales and plots all markets on same scatterplots.
insider_features.py does percentile comparisons between general public and insider for each market in trades_details.csv separately and plots the results.
 
Notes:
- For detect_jump the rolling window is hardcoded to 2 hours.

Suggested directions to go into:
- Figure out what the traders are buying, Yes tokens or No, so we know which side of market they are on, look at documentation on github for huggins.
- Look at features, think of what features we could add, so we have features that set insiders apart from the others.
- Add other potential insider markets, e.g. the other google-related markets the AlphaRaccoon bet on, preferably keep references so we have strong reasons why we chose these markets.
