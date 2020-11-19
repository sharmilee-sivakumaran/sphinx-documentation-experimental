### Porting instructions
To port Event Scrapers from FiscalNote-Legislation to FN-Scrapers, several things need to be done:
1. Create a new folder like `ga_events` under `fn_scrapers\datatypes\events\states`. In that folder create a `__init__.py` file and a file with the scraper code like `ga_events.py`. The first line of the code should be
    ```python
    from __future__ import absolute_import
    ```
2. Replace imports for `EventScraper` and `Event` classes to:
    ```python
    from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
    ```
3. Import `scraper` and `tags` annotations
    ```python
    from fn_scrapers.api.scraper import scraper, tags
    ```
4. Annotate the EventScraper class like this:
    ```python
    @scraper()
    @tags(type="events", group="fnleg", country_code="US", subdivision_code="US-GA")
    class GAEventScraper(EventScraper):
    ```
5. Create a constructor
    ```python
    def __init__(self, *args, **kwargs):
        super(GAEventScraper, self).__init__('ga', __name__, **kwargs)
    ```
6. Scraper events can now be logged to Kibana using the following functions
    ```python
    self.log(message, *args)
    self.info(message, *args)
    self.debug(message, *args)
    self.ok(message, *args, ltype="event_scraper")
    self.warning(message, *args, ltype="event_scraper")
    self.error(message, *args, ltype="event_scraper")
    self.critical(message, *args, ltype="event_scraper")
    self.exception(message, *args, ltype="event_scraper")
    ```
    The `ltype` argument is the `event_type` of the log message. Many scrapers are already using these functions so look for usage of syntax like the following:
    ```python
    self.info("I am a log of %s of this event %s", variable, event)
    ```
    Look for severity of the log message, Many leg event scrapers sent `critical` log messages in place of warnings. Please use the appropriate functions to set the severity.

7. Many of the utility functions have been moved to the `events.common` modules, the `convert_pdf` function is available in `fn_scrapers.common.extraction.textpdf`, so if the scraper is using text extraction using **pdftotext**, it can be imported.
8. All Metadata related functions are available in `fn_scrapers.datatypes.events.common.metadata`, and `fetch_metadata` is now `get_metadata`. The `get_active_sessions` function is called `_get_active_sessions` in FN-Scrapers.
9. Create an entry in [`schedules.yaml`](https://github.com/FiscalNote/FN-Scrapers/blob/master/scrapers/schedules.yaml) and update the existing entry in the mongo DB to disable the scraper from running automatically from **FiscalNote-Legislation** using the mongo shell using
    ```shell
    mongo scrapers
    ```
    from the mongo servers, i.e, `fn-be-mongo-scraper-s01` or `fn-be-mongo-scraper-p01` depending on the environment
    ```
    db.scrapers.update({locality: 'ga'}, {$set: {"active?": false}}, {multi: true, upsert: false})
    ```
    Verify that the update worked using
    ```
    db.scrapers.find({locality: 'ga'}).pretty()
    ```

10. Remove the state tasks from [`tasks.py`](https://github.com/FiscalNote/FiscalNote-Legislation/blob/master/tasks.py) and locality from [`launch.py`](https://github.com/FiscalNote/FiscalNote-Legislation/blob/master/launch.py) in FiscalNote-Legislation, along with the folder for that state.

#### Notes
1. If the scraper is using **InvalidHttpsScraper**, then the `get` and `post` functions should be called with the `verify=False` argument and delete the imports and use of that class.
2. If the scraper is trying to delete a file it downloaded using `self.urlretieve`, then delete the statement that tries to delete the temporary file, it will be deleted automatically after the `convert_pdf` call or when the use of the file is finished.

**NOTE**: Access to document service is available as well in the `fn_scrapers.common.files` module, methods to use that have NOT yet been defined in the base `EventScraper` and `Scraper` class.

## Logging

Logging is available using `BlockingEventLogger` along with the normal python logger. The BlockingEventLogger logs ar esent to Kibana currently, but the python logging messages are only sent to Kibana only in Dev.

Using python logging makes the unit testing a little bit easier, for e.g. **NEEventScraper**.
