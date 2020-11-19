# FN-Scrapers

FN-Scrapers provides a runtime environment for scrapers and a single repository that
many different scrapers can be stored together in.

## Pre-Setup:

The **NH Bill** scraper needs FreeTDS drivers to be present on the machine to run.

For **Mac OS**:
```bash
brew install freetds@0.91
brew link --force freetds@0.91
```
For **Ubuntu**:
```bash
sudo apt-get install freetds-bin freetds-common freetds-dev
```

### To verify that the driver works:
For **Mac OS**:
```bash
TDSVER=4.2 tsql -H 66.211.150.69 -p 1433 -U publicuser -P PublicAccess
```
For **Ubuntu**:
```bash
tsql -H 66.211.150.69 -p 1433 -U publicuser -P PublicAccess
```

The output should be:
```bash
locale is "en_US.UTF-8"
locale charset is "UTF-8"
using default charset "UTF-8"
1> 
```
without any errors. Type `exit` to close the tsql shell.

## Setup

1. `mkvirtualenv scrapers`

2. `pip install --upgrade pip`

3. `pip install Cython==0.27.3`

4. `PKG_VERSION=git pip install -r requirements.txt`

   For **NHBillScraper** install requirements for MS SQL as well:
   `pip install -r requirements_mssql.txt`

5. `pip install -e .`

6. Copy sample-config.yaml to config.yaml. Update the file as desired, but
   most scrapers should run with just the AWS credentials specified.

7. Create a .fn_rabbit.json file if you want to publish to RabbitMQ. If you don't
   want to, no such file is needed.

8. Rename sample-ratelimiter-config.json to ratelimiter-config.json (or, otherwise
   create such a file.)

9. List scrapers: `python -m fn_scrapers scraper list`

10. List scraper options: `python -m fn_scrapers scraper run ExampleScraper --help`

11. Run a scraper: `python -m fn_scrapers scraper run ExampleScraper --message="I'm a scraper"`

## Scraper Specific Configuration

Several scrapers require locality specific secrets (api keys, etc) that are not checked in to git. These are added to the `config.yaml` file. Below is an incomplete list of scraper configurations:

 - `app.scrapers.NYBillScraper.api_key` - string API key.
 - `app.scrapers.NHBillScraper` - dictionary of SQL connection params.
 - `app.scrapers.GermanyRegNoticeScraper` - dictionary of username/password authentication.
 - `app.scrapers.TwitterScraper.twitter` - dictionary of access keys and secrets.

## More Information?

Check out further documentation in the docs directory.