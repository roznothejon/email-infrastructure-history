#!/usr/bin/env python3
"""Proof-of-concept: confirm validate() catches injected bad samples."""

import datetime, sys, os
sys.path.insert(0, os.path.dirname(__file__))
from validate_completeness import validate, _NOT_FOUND

REAL_DOMAIN = '00000055.xyz.'
REAL_DATE   = datetime.date(2025, 11, 3)

samples = [
    # should PASS — real domain, real date, real value
    {
        'domain':     REAL_DOMAIN,
        'query_type': 'TXT',
        'date':       REAL_DATE,
        'raw_value':  '"v=spf1 a mx ip4:5.189.140.138 ~all"',
        'source':     'synthetic/pass',
    },
    # should FAIL — real domain, real date, wrong value
    {
        'domain':     REAL_DOMAIN,
        'query_type': 'TXT',
        'date':       REAL_DATE,
        'raw_value':  '"v=spf1 include:fake-injected.example.com ~all"',
        'source':     'synthetic/wrong-value',
    },
    # should FAIL — completely made-up domain
    {
        'domain':     'totally-fake-domain-that-does-not-exist.example.',
        'query_type': 'MX',
        'date':       REAL_DATE,
        'raw_value':  'mail.fake.example.',
        'source':     'synthetic/missing-domain',
    },
]

result = validate(samples, verbose=True)

print(f"\npassed: {result['passed']}  failed: {result['failed']}  skipped: {result['skipped']}")

assert result['passed']  == 1, f"expected 1 pass,  got {result['passed']}"
assert result['failed']  == 2, f"expected 2 fails, got {result['failed']}"
assert result['skipped'] == 0, f"expected 0 skips, got {result['skipped']}"

print("\nAll assertions passed — validator correctly catches injected errors.")
