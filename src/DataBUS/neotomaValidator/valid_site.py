import DataBUS.neotomaHelpers as nh
from DataBUS import Geog, WrongCoordinates, Site, SiteResponse


def valid_site(cur, yml_dict, csv_file):
    """
    Validate if the provided site details correspond to a new and valid site entry.
    This function checks the validity of a site based on its coordinates, name, and other attributes.
    It returns a SiteResponse object containing:
        - `validAll`: Boolean indicating if the site validation was successful.
        - `sitelist`: List of Site objects that are close to the provided coordinates.
        - `hemisphere`: String indicating the hemisphere of the site based on its coordinates.
        - `message`: List of messages detailing the validation process.

    Args:
        cur (_psycopg2.extensions.connection_): Database connection to a Neotoma database.
        yml_dict (dict): Dictionary containing parameters from YAML configuration.
        csv_file (str): Path to CSV file containing additional parameters.

    Returns:
        SiteResponse: Contains the results of the site validation.
    """
    response = SiteResponse()
    params = ["siteid", "sitename", "altitude",
              "area", "sitedescription", "notes",
              "geog"]

    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.sites")
    overwrite = nh.pull_overwrite(params, yml_dict, "ndb.sites")

    if 'geog.latitude' and 'geog.longitude' in inputs:
        inputs['geog'] = (inputs["geog.latitude"], inputs["geog.longitude"])
        del inputs["geog.latitude"], inputs["geog.longitude"]
    try:
        assert all(
            inputs.get(key) is not None and inputs[key] != []
            for key in ["sitename", "geog"]
        )
    except AssertionError:
        response.message.append(
            f"✗ The template must contain a sitename and coordinates."
        )
        response.valid.append(False)
    try:
        inputs["geog"] = Geog((inputs["geog"][0], inputs["geog"][1]))
        response.message.append(
            f"? This set is expected to be " f"in the {inputs['geog'].hemisphere} hemisphere."
        )
    except (TypeError, WrongCoordinates) as e:
        response.valid.append(False)
        response.message.append(str(e))
        inputs['geog'] = None
    try:
        site = Site(**inputs)
    except (ValueError, TypeError, Exception) as e:
        response.valid.append(False)
        response.message.append(e)
        site = Site(sitename="Placeholder")
    if site.siteid is None:
        close_sites = site.find_close_sites(cur, limit=3)
        if close_sites:
            response.message.append(
                "?  One or more sites exist close to the requested site."
            )
            for site_data in close_sites:
                new_site = Site(
                    siteid=site_data[0],
                    sitename=site_data[1],
                    geog=Geog((site_data[3], site_data[2])),
                )
                new_site.distance = round(site_data[13], 0)
                response.closesites.append(new_site)
            sitenames_list = [site.sitename for site in response.closesites]
            response.matched["namematch"] = any(
                name in sitenames_list for name in site.sitename
            )
            response.matched["distmatch"] = any(
                site.distance == 0 for site in response.closesites
            )
            response.doublematched = (
                response.matched["namematch"] and response.matched["distmatch"]
            )
            match_status = "matches" if response.doublematched else "differs"
            response.message.append(
                f"? Site name {match_status}. Locations differ."
            )
        else:
            response.valid.append(True)
            response.closesites = []
            response.message.append("✔  There are no sites close to the proposed site.")
    else:
        response.message.append(
            "Verifying if the site exists already in neotoma with the same siteID"
        )
        site_query = """SELECT * from ndb.sites where siteid = %(siteid)s"""
        cur.execute(site_query, {"siteid": site.siteid})
        site_info = cur.fetchall()
        if not site_info:
            response.valid.append(False)
            response.message.append(
                f"? Site ID {site.siteid} is not currently associated to a site in Neotoma."
            )
        else:
            response.message.append("✔  Site ID found in Neotoma:")
            for site_data in site_info:
                new_site = Site(
                    siteid=site_data[0],
                    sitename=site_data[1],
                    geog=Geog((site_data[3], site_data[2])),
                    altitude=site_data[6],
                    area = site_data[7],
                    sitedescription=site_data[8],
                    notes=site_data[9])
                msg = site.compare_site(new_site)
                response.message.append(f"? Are sites equal: {site == new_site}.")
                if msg:
                    response.message.append(f"Fields at the Site level differ."
                                            f"Verify that the information is correct.")
                    for i in msg:
                        response.message.append(f"{i}")
                    required = nh.pull_required(params, yml_dict, table="ndb.sites")
                    required_k = [key for key, value in required.items() if value]
                    required_k.remove('geog')
                    found_keywords = [keyword for keyword in required_k if any(keyword in text for text in msg)]
                    marker = bool(found_keywords)
                    if marker:
                        response.message.append(f"Required fields that differ in Neotoma and CSV file as per template: {found_keywords}")
                        response.valid.append(False)
                    else:
                        response.message.append("Some fields differ, but they are not required fields.")
                response.matched["distmatch"] = site.geog == new_site.geog
                response.message.append(new_site)
                if not response.matched["distmatch"]:
                    close_sites = site.find_close_sites(cur, limit=3)
                    response.message.append(
                        f"? Following sites are close to proposed sites."
                    )
                    for site_data in close_sites:
                        close_site = Site(
                            siteid=site_data[0],
                            sitename=site_data[1],
                            geog=Geog((site_data[3], site_data[2])))
                        close_site.distance = round(site_data[13], 0)
                        response.closesites.append(close_site)
                    if not overwrite["geog"]:
                        response.message.append(
                            f"? Coordinates do not match."
                            f"Neotoma coordinates will stay in place. \n"
                            f"Current coords in Neotoma: {new_site.geog}. \n"
                            f"Proposed coords: {site.geog}."
                        )
                    else:
                        response.message.append(
                            "? Coordinates do not match. "
                            f"Proposed coordinates will stay in place. \n"
                            f"Current coords in Neotoma: {new_site.geog}. \n"
                            f"Proposed coords: {site.geog}."
                        )
                else:
                    response.message.append("✔  Coordinates match")
    response.validAll = all(response.valid)
    return response