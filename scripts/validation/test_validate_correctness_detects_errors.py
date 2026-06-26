#!/usr/bin/env python3
"""Proof-of-concept: confirm validate_correctness catches hallucinated events."""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from validate_correctness import validate, sample_events, DATASET_CONFIG

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

cfg        = DATASET_CONFIG['toplists']
events_dir = os.path.join(_REPO_ROOT, cfg['data_dir'], 'events')
sources    = cfg['sources']

# Pull one real event from the event store.
real = sample_events(events_dir, 1, include_disappearances=False)
if not real:
    print("No events found in event store.", file=sys.stderr)
    sys.exit(1)

real_event = real[0]
print(f"Real event: {real_event['domain']}  {real_event['query_type']}  {real_event['measurement_date']}")
print(f"            value={real_event['value']}\n")

samples = [
    # should PASS — real event, real value
    real_event,
    # should FAIL — same domain+date, hallucinated value
    {**real_event, 'value': ['hallucinated-fake.mx.example.com.']},
    # should FAIL — completely invented domain
    {**real_event, 'domain': 'totally-hallucinated-domain.example.com.'},
]

result = validate(samples, sources, verbose=True)

print(f"\npassed: {result['passed']}  failed: {result['failed']}  skipped: {result['skipped']}")

assert result['passed']  == 1, f"expected 1 pass,  got {result['passed']}"
assert result['failed']  == 2, f"expected 2 fails, got {result['failed']}"

print("\nAll assertions passed — validator correctly catches hallucinated events.")
