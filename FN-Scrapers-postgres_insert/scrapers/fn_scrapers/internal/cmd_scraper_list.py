from __future__ import absolute_import

from .find_scrapers import get_scraper_classes
from .scraper_internal import get_scraper_name
from .tableformat.table import TableBuilderBuilder, ASCENDING
from .tableformat.format_text_table import FormatTextTableBuilder
from .tableformat.cli_utils import get_fields, build_eval_include_func, build_filter_include_func
from .tag_util import TagValues, AllTags, get_all_tags


def list_scrapers(args):
    default_fields = [
        "name",
        "group",
        "type",
        "country",
        "subdivision",
        "chamber",
    ]

    tbb = TableBuilderBuilder()
    tbb.add_column("name", lambda row: get_scraper_name(row.data))
    tbb.add_column("tags", lambda row: get_all_tags(row.data))
    tbb.add_column("group", lambda row: row.cells_dict["tags"].value.group)
    tbb.add_column("type", lambda row: row.cells_dict["tags"].value.type)
    tbb.add_column("country_code", lambda row: row.cells_dict["tags"].value.country_code)
    tbb.add_column("country", lambda row: row.cells_dict["tags"].value.country)
    tbb.add_column("chamber", lambda row: row.cells_dict["tags"].value.chamber)
    tbb.add_column("subdivision_code", lambda row: row.cells_dict["tags"].value.subdivision_code)
    tbb.add_column("subdivision", lambda row: row.cells_dict["tags"].value.subdivision)
    tbb.add_sort("group", ASCENDING)
    tbb.add_sort("type", ASCENDING)
    tbb.add_sort("name", ASCENDING)
    if args.filter:
        tbb.add_include_func(build_filter_include_func(["name"], args.filter))
    if args.eval:
        tbb.add_include_func(build_eval_include_func(args.eval))
    tbb.with_fields(get_fields(default_fields, args.fields))
    table_builder = tbb.build()

    fttb = FormatTextTableBuilder()
    fttb.add_formatter(TagValues, lambda x: u", ".join(sorted(x.tag_values)))
    fttb.add_formatter(AllTags, lambda x: u", ".join(sorted(tn + u"=" + tv for tn in x.tags for tv in x.tags[tn])))
    formatter = fttb.build()

    print formatter(table_builder(get_scraper_classes()))
