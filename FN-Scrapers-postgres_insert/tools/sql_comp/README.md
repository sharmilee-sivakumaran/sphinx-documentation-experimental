# SQL Compare Tool

The SQL Compare Tooll is designed to compare one sql environment to another using sqlalchemy orm models, generating a report giving jsonpath's of what changed.

The program has a plugin architecture where simply authoring a plugin module or package will be enough to deploy this for a particular data type. Currently only the `legislation.bills` datatype is included, but it should be relatively straightforward to add more.

## Installation

1. This can be installed either in a new virtualenv or can share with FN-Scrapers.

```bash
# from FN-Scrapers folder
pip install -e tools/sql_comp/
PKG_VERSION=git pip install -r tools/sql_comp/requirements.txt
cp tools/sql_comp/sql_comp/settings.yaml tools/sql_comp/sql_comp/settings.local.yaml
```

2. Update `settings.local.yaml` with your usernames/passwords.

## Usage

The basic usage is as follows:

```bash
python -m sql_comp --help
python -m sql_comp bills --help
```

A complete example:

```bash
> python -m sql_comp bills ga 20172018r "HR 1417"
TIME: 2018-03-14 21:34:21 UTC
LEFT: stephen@rds-ds-atlantisbatchro-p01.cjdtimdpf6hg.us-east-1.rds.amazonaws.com
RIGHT: stephen@fn-atlantis-staging-ro1.cjdtimdpf6hg.us-east-1.rds.amazonaws.com

=== HR 1417 ====================================================================
MOD $.forecast
    L: "0.0439605297713 more_likely"
    R: "0.948355019886 much_more_likely"
ADD $.sponsor_legislators[6]
  "Allen Milne Peake"

=== 0 out of 1 Match ===========================================================
> 
```

## Standard Command-Line Arguments

 - `left`: explicitly state the left database to use. Provide a key entry from `settings.local.yaml`. If not provided, uses the first appropriate database found (production pillar for bills, for example).
 - `right`: explicitly state the right database to use. Same behavior as `--left` but uses the second appropriate database. 
 - `logging`: amount of logging to provide. Set to debug to receive detailed sql query information.
 - `left_only`: do not run a diff report but instead run the query against the left database, outputing the resulting record. Useful to view the record if a diff is unclear.
 - `right_only`: same as `--left_only` but for the right database.
 - `clear_ignored`: clears the default list of jsonpath filters (`.id`, `.created_at`, etc). See `--ignore` for syntax and further details.
 - `ignore`: append one or more values to the jsonpath filters, combine this with `--clear_ignored` to have a custom set of filters. A given value will be compared to the jsonpath, and if the jsonpath ends in the value the record is not compared. For example, `.id` will filter out changes to `$.id` and `$.legislator.id` but not `$.legislator.ideology`. An alternate syntax is to prepend the value with a tilde for contains-matching rather than endswith matching: `.similarities[` will filter results such as `$.similarities[0].value` and `$.similarities[1].value`.
 - `show_same`: Also report matching records. Will report something like `"{identifier} Okay"`.
 - `format`: Change output formatting (currently default and `json` is supported).

## Extensibility

The layout of the program is designed to be modular, with each datatype consisting of a module or package located in the `plugins` folder. Once the file is added, no additional configuration is needed.

The basic datatype is a class that inherits from the `SqlComp` class, and this is what the fraework will seek out when executing. The child class can have the following variables/methods:

### `self.__init__(*args, **kwargs)` - Constructor

Use to set instance variables *after* calling the paerent constructor (see below for a list). Also pass a `env_type` keyword argument to the parent constructor to specify a default environment type for automatic loading of databases.

### `self.trimmings` - SQL Model Limitations/Representations

The challenge with writing an ORM spider is that the entire database may be loaded with a simple query. For instance, a bill will link to a sesison which links to additional bills. The `trimmings` collection specifies how to handle *non-root* objects in a way to prevent over-crawling (the crawler will already avoid recursion).

Further, even if following a datatype would not be overly-greedy `trimmings` can be used to allow for a more human-friendly output. For example, having a committee report back as `"Senate Finance Committee"` as opposed to a complex data object with a title somewhere in there is more user friendly and decreases the odds of a false-positive.

The data-structure is a dictonary. The key should be set to the SQLAlchemy ORM class (Bill or Committee, see FN-Pillar-Models repository). The value is a one-argument function (or lambda) which accepts a record (instantiated model of the same type as the key) and returns a json-friendly data structure (string, number, bool, list or dictionary with acceptable primitives).

For example, given the committee example from before, the `trimmings` value could look like this:

```python
    from fn_pillar_models.legislation.committee import Committee

    # ...

    self.trimmings = {
        Committee: lambda r: r.name
    }
```

### `self.json_filters` - Model Specific jsonpath Filters

This is a collection of json_filters specific to the specified data model. Be sure to append (`+=`)when appropriate:

```python
    self.json_filters += ('.foo', '.bar')
```

### `self.node_filter` - Leaf Filtering on Crawl

This allows the establishment of logical rules to filter nodes (for instance, if an `is_active` field is False, don't include this node). This differs from `trimmings` in that complex logic can be used. If this function returns True, the object will not be included in the query output.

### `self.parser(config, parser)` - Additional Command Line Arguments

Receives a `argparse.subparser` instance to add arguments to (the subparser is set to use the module/package name as the identifier). Be sure to call the parent method to set the general command line arguments.

### `self.run()` - Run The Comparison

Most basic use is to simply call the inherited `self.diff()` in here with the following parameters:

 - `obj` - The SQLAlchemy model to scrape.
 - `ident` - A string to define identity (`"name"` or `"external_id"` are good choices, '"id"` not so much as that will rarely match across environments).
 - `filters` - A list of strings to filter the result by (different from `json_filters` above). These are used to construct the SQL Where clause. Examples include `"session == 20172018r"` or `'external_id in ["HR 123", "HR 456"]'`. Accepted operators are `==`, `=`, `!=`, `<`, `>`, `<=`, `>=`, and `in`. Calling `json.dumps` is useful for formatting lists. 
