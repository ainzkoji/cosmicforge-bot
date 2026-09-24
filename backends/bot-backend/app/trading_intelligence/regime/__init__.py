"""Deterministic V1 probabilistic regime model (Section 10) -- built strictly
on top of MarketState. Six regime weights, entropy, transition uncertainty,
evidence attribution and specialist-routing eligibility. Never places an
order, never reads tenant state, never forces a hard label as sole truth."""
