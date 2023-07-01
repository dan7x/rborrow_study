import pandas as pd

x = {
  "events":[
    {
      "event_type":"creation",
      "occurred_at":1589382308.0,
      "creation_type":0,
      "creation_permalink":"https://reddit.com/comments/fex5x6/redditloans/fqhqena"
    },
    {
      "event_type":"repayment",
      "occurred_at":1589555293.363745,
      "repayment_minor":100
    },
    {
      "event_type":"repayment",
      "occurred_at":1589555600.049615,
      "repayment_minor":22
    },
    {
      "event_type":"repayment",
      "occurred_at":1589555998.253033,
      "repayment_minor":100
    },
    {
      "event_type":"repayment",
      "occurred_at":1589556300.691426,
      "repayment_minor":4778
    },
    {
      "event_type":"admin",
      "occurred_at":1592234695.896887,
      "admin":"tjstretchalot",
      "reason":"testing",
      "old_principal_minor":5000,
      "new_principal_minor":4000,
      "old_principal_repayment_minor":5000,
      "new_principal_repayment_minor":4000,
      "old_created_at":1589382308.0,
      "new_created_at":1589382308.0,
      "old_repaid_at":1589556300.691426,
      "new_repaid_at":1592234695.905152,
      "old_unpaid_at": None,
      "new_unpaid_at": None,
      "old_deleted_at": None,
      "new_deleted_at": None
    }
  ],
  "basic":{
    "lender":"tjstretchalot",
    "borrower":"tjstretchalot",
    "currency_code":"EUR",
    "currency_symbol":" EUR",
    "currency_symbol_on_left": False,
    "currency_exponent":2,
    "principal_minor":4000,
    "principal_repayment_minor":4000,
    "created_at":1589382308.0,
    "last_repaid_at":1589556300.691426,
    "repaid_at":1592234695.905152,
    "unpaid_at": None,
    "deleted_at": None
  }
}

df = pd.DataFrame(x)
breakpoint()