# Slack Scan 

Aggregrates Slack Messages in a channel based on regex rules

## Installation

1. From a new virtual environment:

```
pip install -r requirements.txt
cp settings.yaml setttings.local.yaml
```

2. Invite the slack bot `di_event_summary` into the channel you wish to monitor.

3. Configure `settings.local.yaml` as follows:
    - Under channels add your slack channel name as a key. 
    - For the channel add a `pattern` key which will contain a list of patterns.
    - Each pattern entry should have a `name` and `pattern` regex pair. If a message matches a pattern it will be counted. 
    - If you wish to apply further filtering, include a `format` field that will take the given `pattern`'s match and apply the format string (`"{0}"` will match teh first subpattern, or the entire pattern if no subpatterns are defined). 

## Execution

```
python slack_scan.py
```

### Options:

`days` (default 1): How many days to scan for slack messages. One day is 24 hours (not calendar days).
`count` (default 10): How many of the top results should be shown for each count.
`channel` (default: di_events): The channel to scan.