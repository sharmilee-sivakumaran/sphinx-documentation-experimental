# HTTP Monitor

Runs a script through doc service to monitor for changes. Once a change is noticed, sends an alert through slack.

## Configuration

AWS credentials are loaded from the `~/.boto` file (first found set used). The service will be deployed to fn-pillar-scraper-d01 so the dev environment credentials will be used.

The S3 and document service details are defined in the `settings.yaml` file.

The slack webhook is also in `settings.yaml`

The websites to be monitored are defined by in `settings.yaml` as a `name`/`url` pair. Addional logic could be applied by using a `type` keyword (not currently implemented).

The program will be regularly run using `cron`.

## Implementation Notes:

Webhook included in the config file as Events Engine also has this practice. No information is discoverable through this, just annoying/DOS if revealed. Can easily be revoked/changed.

Contains copy of `common.http` and `common.files` instead of linking. I chose this method for two reasons: 1) freezes the interface for this low-importance task, and 2) decouples this from fn-scrapers. Can be updated via `diff`/`cp`.

Currently no requirements defined. FN-Scrapers virtual environment should be enough (or any scraper virtual environment).

## Future Expansion Ideas:

 - Define individual time frequencies in `websites`.
 - Define additional hooks with a default hook, allowing websites to reference to specialized hooks.
 - Define additional diff logic such as HTML-diff where an xpath is provided for comparison.
 - Define whatever your heart desires.
