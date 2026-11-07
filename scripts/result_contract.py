"""Worker result validation and shared attention policy.

Workers return only their status and answer. The scheduler never interprets
the business content of an answer.
"""

RESULT_STATES = ('complete', 'partial', 'blocked')
RESULT_FIELDS = {'status', 'answer'}


def valid_result(value):
    return (isinstance(value, dict) and set(value) == RESULT_FIELDS
            and value['status'] in RESULT_STATES and isinstance(value['answer'], str))


def result_issues(value):
    issues = value.get('issues', [])
    return [v for v in issues if isinstance(v, str) and v.strip()] if isinstance(issues, list) else []


def needs_attention(row):
    """Lifecycle/evidence failures and actionable issues, never research limits."""
    return (row.get('status') not in ('complete', 'running', 'queued', 'retrying', 'starting')
            or bool(result_issues(row))
            or row.get('result_observation') in ('unavailable', 'invalid')
            or row.get('execution') in ('failed', 'interrupted', 'unknown')
            or (row.get('execution') == 'succeeded' and row.get('reported_status') == 'unknown'))
