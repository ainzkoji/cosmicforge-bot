"""Legal documents and consent (Section H Step 8.2).

* :mod:`shared_lib.legal.schema`    -- tables + integrity triggers, called from ``migrate()``
* :mod:`shared_lib.legal.documents` -- publish / current / accept / withdraw / re-consent / eligibility
* :mod:`shared_lib.legal.seed`      -- clearly-labelled DRAFT seed documents only

No document is ever counsel-approved by default; approval is recorded only
through the admin API with a reviewer name and reference.
"""
