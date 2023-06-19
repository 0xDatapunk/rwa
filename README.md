# rwa

- Instructions to run:

    Import token_transfers.py to databricks and run as a notebook

- Considerations:

    The transfers excludes Mint(), Burn(), and self-transfer (need to understand this better), but includes transfers from A to B, then to C within the same transactions as two separate transfers. 

    To correctly modify this behavior requires closer examination of different protocols and understand how to remove duplicates, for example, uniswap. 

    The top token per week itself might not change, given the usage of WETH in a lot of transactions, but the count/sum values would be quite smaller.


- How to offer these outputs as an API for multiple time frames (1D, 1W, 1M, etc.)

    As part of the medallion architecture, a gold table is to be stored with top transferred tokens with multiple time frames. Depending on performance requirement, this table can potentially be used to serve the API call. 

    Use popular python web framework (FastAPI or Flask, as examples), one can retrieve the data from above table and return the result through the API call. See pseudocode example in `api.py`

###