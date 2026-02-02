We want to use Polymarket to make abnormal positive returns in established financial markets. 
We wish to explore and backtest the following ideas: 
    1. Insiders: ML model to exploit insider trading on Polymarket.
    2. Probabilities: Use Polymarket probabilities to update current beliefs, e.g. Volatility Forecast.

1. Insiders
    a. Calibrate model that detects insider trading in Polymarket.
    b. Make NLP model that chooses relevant Polymarket conditionIds associated with the instrument of interest.
    c. Scan the market on a rolling window to detect insiders in relevant markets.
    d. Make trades on the instrument of interest once insider detected. 

2. Probabilities

    a. NLP

Can be used with the first one to build volatility predictions. Use Polymarket probabilities as a 'long-run expectation', and then use the insider trading for intraday trading.

Assumptions:
    1. Insider traders execute trades at least 24 hours before the event.
    2. Insider traders are putting in trades of at least 1000$
    3. Volume of the market is at least 150,000 USD

Markets with insider trading:

OpenAI browser by October 31? 0xc25e15f39f776813870ee363ed483451add1c55dad163b18bdb2df653be2c90c

Maduro out by January 31? 0x580adc1327de9bf7c179ef5aaffa3377bb5cb252b7d6390b027172d43fd6f993

Israel military action against Iran by Friday?,0x7f39808829da93cfd189807f13f6d86a0e604835e6f9482d8094fac46b3abaac

Israel announces end of military operations against Iran before July?
0x836e01d34c67bbae976e7dd26b5ef2ec5e0f15f261f62ebae3d3ef4de32258c0
