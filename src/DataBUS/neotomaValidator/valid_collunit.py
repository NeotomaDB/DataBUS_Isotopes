import itertools
import re
import DataBUS.neotomaHelpers as nh
from DataBUS import Geog, WrongCoordinates, CollectionUnit, CUResponse


def valid_collunit(cur, yml_dict, csv_file):
    """
    Validates whether the specified collection unit can be registered as a new unit in the Neotoma database.

    Args:
        cur (psycopg2.extensions.connection): A database connection object to interact with a Neotoma database, which can be either local or remote.
        yml_dict (dict): A dictionary containing data from a YAML template.
        csv_file (str): The path to a CSV file with the required data to be uploaded.

    Returns:
        CUResponse: An object containing the validation response with the following attributes:
            - valid (bool): Indicates whether the collection unit passed the validation checks.
            - message (list): A list of messages detailing the validation process.
            - culist (list): A list of dictionaries containing site and collection unit names that are valid within the specified site context.
    """
    response = CUResponse()
    params = ["handle", "core", "depenvtid", "collunitname",
              "colldate", "colldevice", "gpsaltitude", "gpserror",
              "waterdepth", "substrateid", "slopeaspect", "slopeangle",
              "location", "notes", "geog", "colltypeid"]
    try:
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.collectionunits")
    except Exception as e:
        error_message = str(e)
        try:
            if "time data" in error_message.lower():
                event_dates = [item.get('eventDate') for item in csv_file if 'eventDate' in item]
                new_date = list(set(event_dates))
                assert len(new_date) == 1, "There should only be one date"
                new_date = new_date[0]
                if isinstance(new_date, str) and len(new_date) > 4:
                    if len(new_date) == 7 and new_date[4] == '-' and new_date[5:7].isdigit():
                        new_date = f"{new_date}-01"
                    elif new_date.endswith("--"):
                        new_date = None
            params.remove("colldate")
            inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.collectionunits")
            inputs["colldate"] = new_date
            response.valid.append(True)
        except Exception as inner_e:
            response.validAll = False
            response.message.append("CU parameters cannot be properly extracted. {e}\n")
            response.message.append(str(inner_e))
            return response
 
    if inputs["colltypeid"]:
        colltype_query = """SELECT colltypeid FROM ndb.collectiontypes 
                            WHERE LOWER(colltype) = %(colltype)s"""
        cur.execute(colltype_query, {'colltype': inputs['colltypeid'].lower()})
        inputs["colltypeid"] = cur.fetchone()

    if inputs["colltypeid"]:
        inputs["colltypeid"] = inputs["colltypeid"][0]

    if inputs['substrateid']:
        query = """SELECT rocktypeid FROM ndb.rocktypes
                    WHERE LOWER(rocktype) = %(rocktype)s"""
        cur.execute(query, {"rocktype": inputs["substrateid"].lower()})
        substrate = cur.fetchone()

        if substrate:
            inputs["substrateid"] = substrate[0]
        else:
            response.message.append(f"No substrate {inputs['substrateid']} found")
            inputs["substrateid"] = None
            
    if inputs["depenvtid"]:
        query = """SELECT depenvtid FROM ndb.depenvttypes
                    WHERE LOWER(depenvt) = %(depenvt)s"""
        cur.execute(query, {"depenvt": inputs["depenvtid"].lower()})
        depenv = cur.fetchone()
        if depenv:
            inputs["depenvtid"] = depenv[0]
        else:
            response.message.append(f"Depositional environment {inputs['depenvtid'].lower()} "
                                    f"not found in Neotoma.\n"
                                    f"Depositional environment will be added to Notes.")
            inputs["depenvtid"] = None
    
    if inputs['geog']:
        try:
            inputs['geog'] = Geog((inputs["geog"][0], inputs["geog"][1]))
            response.message.append(
                f"? This set is expected to be " f"in the {geog.hemisphere} hemisphere."
            )
        except (TypeError, WrongCoordinates) as e:
            response.valid.append(False)
            response.message.append(str(e))
            inputs['geog'] = None
    else:
        inputs['geog'] = None

    try:
        cu = CollectionUnit(**inputs)
        response.valid.append(True)
    except (ValueError, TypeError, Exception) as e:
        response.valid.append(False)
        response.message.append(e)
        cu = CollectionUnit(handle="Placeholder")

    response.message.append(f"Handlename: {cu.handle}")
    if inputs["handle"] != cu.handle:
        response.message.append(f"? Handlename not given. Handle created"
                                f" from core code.")
    cur.execute(
        "SELECT * FROM ndb.collectionunits WHERE LOWER(handle) = %(handle)s;",
        ({"handle": cu.handle.lower()}))
    rows = cur.fetchall()
    if not rows:
        response.message.append("✔  No handle found. Creating a new collection unit.")
        response.valid.append(True)
    else:
        response.message.append("? There is a handle with this handle name.")
        if len(rows) == 1:
            coll_info = rows[0]
            try:
                inputs2 = {'collectionunitid': int(coll_info[0]),
                           'handle': str(coll_info[1]),
                           'siteid': int(coll_info[2]),
                           'colltypeid': int(coll_info[3]),
                           'depenvtid': int(coll_info[4]),
                           'collunitname': str(coll_info[5]),
                           'colldate': coll_info[6],
                           'colldevice': str(coll_info[7]),
                           'geog': Geog(coll_info[8], coll_info[9]),
                           'gpsaltitude': int(coll_info[10]),
                           'gpserror': coll_info[11],
                           'waterdepth': float(coll_info[12]),
                           'substrateid': int(coll_info[13]),
                           'slopeaspect': coll_info[14],
                           'slopeangle': coll_info[15],
                           'location': str(coll_info[16]),
                           'notes':str(coll_info[17])}
                found_cu = CollectionUnit(**inputs2)
                msg = cu.compare_cu(found_cu)
                response.message.append(f"? Are CollUnits equal: {cu == found_cu}.")
                if msg:
                    response.message.append(f"Fields at the CU level differ.\n"
                                            f"Verify that the information is correct.")
                    for i in msg:
                        response.message.append(f"{i}")
                    
                    required = nh.pull_required(params, yml_dict, table="ndb.collectionunits")
                    required_k = [key for key, value in required.items() if value]
                    found_keywords = [keyword for keyword in required_k if any(re.search(rf'CSV\s+\b{re.escape(keyword)}\b', text) for text in msg)]
                    csv_nonempty_fields = [key for key, value in inputs.items() if value not in (None, 'NA')]
                    found_keywords2 = [keyword for keyword in csv_nonempty_fields if any(re.search(rf'CSV\s+\b{re.escape(keyword)}\b', text) for text in msg)]
                    found_keywords = list(set(found_keywords+found_keywords2))

                    if 'geog' in found_keywords:
                        found_keywords.remove('geog')
                    marker = bool(found_keywords)
                    if marker:
                        response.message.append(f"REQUIRED or CSV GIVEN FIELDS differ in Neotoma and CSV file: {found_keywords}")
                        response.valid.append(False)
                        if 'depenvtid' in found_keywords:
                            response.message.append(f"Depenv value in Neotoma is: {found_cu.depenvtid}")
                    else:
                        response.message.append("Some fields differ, but they are not required fields.")     
            except Exception as e:
                response.message.append(e)
                response.valid.append(False)
    
    if inputs['geog']:
        close_handles = cu.find_close_collunits(cur)
        if len(close_handles) > 0:
            goodcols = [i[-2] for i in close_handles]
            if not any([j == cu.handle for j in goodcols]):
                response.message.append(f"?  The collection unit handle does not occur "
                                        f"within close sites.")
                sitecol = itertools.groupby([{"sitename": k[1], "collunit": k[-2]} for k in close_handles],
                                            lambda x: x["sitename"])
                sitemsg = [{"site": key, "collunits": [k["collunit"] for k in list(value)]}
                           for key, value in sitecol]
                for i in sitemsg:
                    site = {"site": i["site"], "collunits": i["collunits"]}
                    response.culist.append(site)
        else:
            response.message.append(
                f"✔  There are no nearby sites, a new collection unit "
                f"will be created."
            )
            response.valid.append(True)
    else:
        response.message.append(f"No given coordinates for CU. Cannot find nearby CUs")
        response.valid.append(True)
    response.validAll = all(response.valid)
    return response