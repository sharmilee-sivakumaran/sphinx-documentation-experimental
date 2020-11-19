import lxml

HI_URL_BASE = "http://capitol.hawaii.gov"
SHORT_CODES = "%s/committees/committees.aspx?chamber=all" % (HI_URL_BASE)

COMMITTEE_MAPPING = {
    "AGR": "House Committee on Agriculture",
    "CPC": "House Committee on Consumer Protection & Commerce",
    "CPH": "Senate Committee on Commerce, Consumer Protection, and Health",
    "EDB": "House Committee on Economic Development & Business",
    "EDN": "House Committee on Education",
    "EDU": "Senate Committee on Education",
    "EEP": "House Committee on Energy & Environmental Protection",
    "EET": "Senate Committee on Economic Development, Environment, and Technology",
    "FIN": "House Committee on Finance",
    "GVO": "Senate Committee on Government Operations",
    "HEA": "Senate Committee on Higher Education and the Arts",
    "HED": "House Committee on Higher Education",
    "HLT": "House Committee on Health",
    "HMS": "Senate Committee on Human Services",
    "HOU": "Senate Committee on Housing",
    "HSG": "House Committee on Housing",
    "HUS": "House Committee on Human Services",
    "HWN": "Senate Committee on Hawaiian Affairs",
    "JDL": "Senate Committee on Judiciary and Labor",
    "JUD": "House Committee on Judiciary",
    "LAB": "House Committee on Labor & Public Employment",
    "LMG": "House Committee on Legislative Management",
    "OHM": "House Committee on Ocean, Marine Resources, & Hawaiian Affairs",
    "PBS": "House Committee on Public Safety",
    "PSM": "Senate Committee on Public Safety, Intergovernmental, and Military Affairs",
    "TOU": "House Committee on Tourism",
    "TRE": "Senate Committee on Transportation and Energy",
    "TRN": "House Committee on Transportation",
    "TSI": "Senate Committee on Tourism and International Affairs",
    "VMI": "House Committee on Veterans, Military, & International Affairs, & Culture and the Arts",
    "WAL": "House Committee on Water & Land",
    "WAM": "Senate Committee on Ways and Means",
    "WLA": "Senate Committee on Water, Land, and Agriculture"
}

def get_short_codes(scraper):
    resp = scraper.get(SHORT_CODES)
    list_html = resp.text
    list_page = resp.lxml()
    rows = list_page.xpath(
        "//table[@id='ctl00_ContentPlaceHolderCol1_GridView1']/tr")
    scraper.short_ids = {
        "CONF": {
            "chamber": "joint",
            "name": "Conference Committee",
        },
    }

    for row in rows:
        tds = row.xpath("./td")
        short = tds[0]
        clong = tds[1]
        chamber = clong.xpath("./span")[0].text_content()
        clong = clong.xpath("./a")[0]
        short_id = short.text_content().strip()
        ctty_name = clong.text_content().strip()
        chmbr= "joint"
        if "house" in chamber.lower():
            chmbr = 'lower'
        elif "senate" in chamber.lower():
            chmbr = 'upper'

        scraper.short_ids[short_id] = {
            "chamber": chmbr,
            "name": ctty_name
        }
