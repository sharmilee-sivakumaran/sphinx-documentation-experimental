from __future__ import absolute_import

import html5lib


def decode_html_entities(text):
    """
    Decode HTML5 entities. We use html5lib because it knows how to handle all entities defined
    by the HTML5 spec: https://html.spec.whatwg.org/, specifically those in
    https://html.spec.whatwg.org/entities.json, while other methods were found
    to miss some of these.

    For example: "how MPC&#39;s apply" -> "how MPC's apply"
    """

    # We do a parseFragment specifying that the container is a "pre" tag since this
    # disables whitespace collapsing. Without this option, html5lib collapses
    # &NewLine; and &Tab; into empty strings.
    return u"".join(html5lib.parseFragment(text, "pre").itertext())
