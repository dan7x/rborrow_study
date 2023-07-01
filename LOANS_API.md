# LoansBot API Compliance

This file exists to comply with [certain requests](https://github.com/LoansBot/web-backend/blob/master/API.md) 
from the API admins.

This program is written in Python as part of a side-project
comparing online lending to conventional lending. 

The bot will mostly send a small volume 
of requests to pull data from a few days by first pulling the loan ID, and then iterating through 
the IDs using a single persistent HTTP request (backing off for 61 seconds as required).

Occasionally, a historical ingest may be required, which will be a burst of requests lasting a few hours 
followed by silence. 

If this ends up being deployed somewhere, it will be run indefinitely, 
likely making a small volume of requests once per day.
