import DataBUS.neotomaHelpers as nh
from DataBUS import Geog, WrongCoordinates, Site, SiteResponse
import importlib.resources
 
with importlib.resources.open_text("DataBUS.sqlHelpers", "upsert_site.sql") as sql_file:
    upsert_query = sql_file.read()

def insert_site(cur, yml_dict, csv_file):
    """Insert or update a site in the Neotoma Paleoecology Database.

    This function inserts a new site or updates an existing site in the Neotoma
    Paleoecology Database based on the given YAML dictionary and CSV file. It handles
    geolocation validation, attribute cleaning, and logging of the operation's
    success or failure.

    Args:
        cur (psycopg2.extensions.cursor): A cursor pointing to the Neotoma
            Paleoecology Database.
        yml_dict (dict): A dictionary returned by the YAML template.
        csv_file (dict): The CSV file with the required data to be uploaded.

    Returns:
        SiteResponse: An object containing information about the inserted or updated site.
            - siteid (int): ID of the inserted or updated site.
            - valid (bool): Indicates if insertions were successful.
            - message (list): List of messages describing the operations performed.
            - sitelist (list): List of site objects involved in the operation.
    """
    response = SiteResponse()
    params = ["siteid", "sitename", "altitude", "area",
              "sitedescription", "notes", "geog"]
    
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.sites")
    overwrite = nh.pull_overwrite(params, yml_dict, "ndb.sites")
    if 'geog.latitude' and 'geog.longitude' in inputs:
        inputs['geog'] = (inputs["geog.latitude"], inputs["geog.longitude"])
        del inputs["geog.latitude"], inputs["geog.longitude"]

    try:
        inputs["geog"]  = Geog((inputs["geog"][0], inputs["geog"][1]))
    except (TypeError, WrongCoordinates) as e:
        response.valid = False
        response.message.append(str(e))
        inputs["geog"]  = None

    try:
        site = Site(**inputs)
        response.valid.append(True)
    except (ValueError, TypeError, Exception) as e:
        response.valid.append(False)
        response.message.append(e)
        site = Site(sitename="Placeholder")

    if site.siteid is not None:
        response.message.append(f"Site ID has been given: {site.siteid}")
        response.siteid = site.siteid
        cur.execute(
            """SELECT * from ndb.sites where siteid = %(siteid)s""",
            {"siteid": response.siteid},
        )
        site_info = cur.fetchall()
        if len(site_info) == 1:
            site_info = site_info[0]
            response.message.append(f"✔  Site ID {response.siteid} found in Neotoma.")
            found_site = Site(
                siteid=int(site_info[0]),
                sitename=site_info[1],
                geog=Geog((float(site_info[3]), float(site_info[2]))),
            )
            # response['sitelist'].append(site)
            updated_site = site.update_site(found_site, overwrite, response)
            updated_site.siteid = found_site.siteid
            response.sitelist.append(
                {"original site": f"{site}", "updated site": f"{updated_site}"}
            )
            cur.execute(upsert_query)  # Defines upsert_site SQL function
            response.siteid = updated_site.upsert_to_db(cur)
            response.valid.append(True)
        elif len(site_info) == 0:
            if overwrite["sitename"] == True:
                response.valid.append(True)
                response.message.append(
                    f"✔  Overwrite is set to True. Site ID {response.siteid} is not currently associated to a site in Neotoma. New Site ID will be given."
                )
            else:
                response.valid.append(False)
                response.message.append(
                    f"✗  Overwrite is set to False. Site ID {response.siteid} is not currently associated to a site in Neotoma."
                )
            response.siteid = site.insert_to_db(cur)
    else:
        response.message.append(f"A new site ID will be generated")
        try:
            response.siteid = site.insert_to_db(cur)
            response.sitelist.append(site)
            response.valid.append(True)
            response.message.append(f"✔  Added Site {response.siteid}")
        except Exception as e:
            response.message.append(f"✗  Cannot add Site: {e}")
            temp_site = Site()
            response.siteid = temp_site.insert_to_db(cur)
            response.valid.append(False)
            response.sitelist.append(temp_site)
    response.validAll = all(response.valid)
    return response