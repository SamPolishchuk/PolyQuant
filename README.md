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

We use the Black-Letterman QEPM portfolio structure to update our beliefs on certain securities that we hold.

    a. We first select our securities using z-scores on momentum.
    b. We find the covariance matrix (sigma) with Pearson's cov matrix (shrinkage preferred) and expected returns (mu) using
        i. Current market protfolio: μ= λΣw
        ii. Fama-French 3F: Essentially arguing the market is somewhat inefficient, and factors explain equilibrium better​
    c. Find the relevant events for these securities, and their associated probabilities on Polymarket.
    d. Somehow aggregate these (or select a range of them) that will be inputed in the views matrices:
        i. Q (view vector): Q=E[r∣event]−E[r], where Expected return impact=P(event)×β_{asset,event}​×shock magnitude; Eg. “If Israel strikes Iran, defense stocks +5%, airlines −3%”
        ii. Omega (uncertainty): (σ_asset)^2 × f(time to resolution)×g(liquidity), e.g. Omega = sigma^2 * (T/T_max)
        iii. P (asset exposure matrix): assets involved in the view (weighted with )
        iv. Tau (uncertainty in the prior mu): between 0.1 and 0.05
    e. Rebalance weekly, and if we detect an insider trader

Overall:
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
