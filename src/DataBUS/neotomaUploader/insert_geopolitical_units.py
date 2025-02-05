import DataBUS.neotomaHelpers as nh
from DataBUS import Response

def insert_geopolitical_units(cur, yml_dict, csv_file, uploader):
    """_Validating geopolitical units"""
    response = Response()

    params = ["geopoliticalunit1"]#, "geopoliticalunit2",
             # "geopoliticalunit3", "geopoliticalunit4"]

    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.sitegeopolitical")
    query = """SELECT geopoliticalid FROM ndb.geopoliticalunits
                WHERE LOWER(geopoliticalname) = %(geopoliticalname)s"""
    
    geo_units = {}
    for unit in inputs:
        if inputs[unit]:
            cur.execute(query, {'geopoliticalname' : inputs[unit].lower()})
            answer = cur.fetchone()
            if not answer:
                answer = None
            else:
                answer = answer[0]
        else:
            answer = None
        geo_units[unit] = answer
    key_params = list(inputs.keys())
    result = next((geo_units[key] for key in key_params[::-1] if geo_units[key] is not None), None)

    if any(value is not None for value in geo_units.values()):
        response.valid.append(True)
        query = """SELECT ts.insertsitegeopol(_siteid:= %(siteid)s,
                                              _geopoliticalid := %(geopolid)s)
                """
        try:
            cur.execute(query, {'siteid': uploader["sites"].siteid,
                                'geopolid': result})
            response.message.append(f"✔ Site SiteID {uploader['sites'].siteid}, GPUID {result} added.")
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗ Site SiteID {uploader['sites'].siteid}, GPUID {result} cannot be added. Verify the format {e}.")
    else:
        response.message.append(f"? Site GPUID not available in Neotoma.")
        response.valid.append(True)

    response.validAll = all(response.valid)
    return response