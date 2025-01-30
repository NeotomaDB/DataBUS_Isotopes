import DataBUS.neotomaHelpers as nh
from DataBUS import Geog, WrongCoordinates, CollectionUnit, CUResponse
import datetime 
import importlib.resources
with importlib.resources.open_text("DataBUS.sqlHelpers", 
                                   "upsert_collunit.sql") as sql_file:
    upsert_query = sql_file.read()

def insert_collunit(cur, yml_dict, csv_file, uploader):
    """_Insert a new collection unit to a site_
    Args: 
        cur (_psycopg2.extensions.cursor_): _A cursor pointing to the Neotoma
            Paleoecology Database._
        yml_dict (_dict_): _A `dict` returned by the YAML template._
        csv_file (_dict_): _The csv file with the required data to be uploaded._
        uploader (_dict_): A `dict` object that contains critical information about the
          object uploaded so far.

    Returns:
        _int_: _The integer value of the newly created siteid from the Neotoma Database._
    """
    response = CUResponse()
    notes = ""
    params = ["collectionunitid", "handle", "core", "depenvtid", "colltypeid",
              "collunitname", "colldate", "colldevice", "gpsaltitude",
              "gpserror", "waterdepth", "substrateid", "slopeaspect",
              "slopeangle", "location", "notes", "geog", "geog.latitude", 
              "geog.longitude"]
    
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
                        new_date = new_date.replace('/', '-')
                        datetime.strptime(date_string, "%Y-%m-%d")
                        notes = notes + f""
                    elif new_date.endswith("--") or new_date.endswith("//"):
                        notes = f"Collection Date seems to be: {new_date}"
                        new_date = None
                    else:
                        notes = notes + f""
            params.remove("colldate")
            inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.collectionunits")
            inputs["colldate"] = new_date
            if not inputs["notes"]:
                inputs["notes"] = notes
            else:
                inputs["notes"] = inputs["notes"] + notes
            response.valid.append(True)
        except Exception as inner_e:
            response.validAll = False
            response.message.append(f"CU parameters cannot be properly extracted. {e}\n"
                                    f"{inner_e}")
            return response
    
    if inputs["colltypeid"]:
        query1 = """SELECT colltypeid FROM ndb.collectiontypes 
                    WHERE LOWER(colltype) = %(colltype)s"""
        cur.execute(query1, {'colltype': inputs['colltypeid'].lower()})
        inputs["colltypeid"] = cur.fetchone()
        if inputs["colltypeid"]:
            inputs["colltypeid"] = inputs["colltypeid"][0]
    
    if 'geog.latitude' and 'geog.longitude' in inputs:
        inputs['geog'] = (inputs["geog.latitude"], inputs["geog.longitude"])
        del inputs["geog.latitude"], inputs["geog.longitude"]
    
    if inputs['geog']:
        try:
            inputs['geog'] = Geog((inputs["geog"][0], inputs["geog"][1]))
        except (TypeError, WrongCoordinates) as e:
            response.valid.append(False)
            response.message.append(str(e))
            inputs['geog'] = None
    else:
        inputs['geog'] = None

    overwrite = nh.pull_overwrite(params, yml_dict, "ndb.collectionunits")

    if inputs["depenvtid"]:
        query = """SELECT depenvtid FROM ndb.depenvttypes
                   WHERE LOWER(depenvt) = %(depenvt)s"""
        cur.execute(query, {"depenvt": inputs["depenvtid"].lower()})
        depenv = cur.fetchone()
        if depenv:
            inputs["depenvtid"] = depenv[0]
        else:
            if inputs["notes"]:
                inputs["notes"] = inputs["notes"] + f"Dep. Env.{inputs['depenvtid']}"
                inputs["depenvtid"] = None
            else:
                inputs["notes"] = f"Dep. Env.{inputs['depenvtid']}"
                inputs["depenvtid"] = None
    if isinstance(inputs["handle"], list):
        response.handle = inputs["handle"][0]
    else:
        response.handle = inputs["handle"]
    try:
        inputs['siteid'] = uploader["sites"].siteid
        cu = CollectionUnit(**inputs)
        response.valid.append(True)
        response.message.append("✔  Added Collection Unit")
    except Exception as e:  
        cu = CollectionUnit(
            siteid=uploader["sites"].siteid, handle="Placeholder", geog=inputs['geog']
        )
        response.valid.append(False)
        response.message.append("✗ CU cannot be created {e}")
    if cu.handle:
        cur.execute(
            """SELECT * FROM ndb.collectionunits WHERE LOWER(handle) = %(handle)s""",
            {"handle": cu.handle.lower()},
        )
        coll_info = cur.fetchall()
        if len(coll_info) == 1:
            coll_info = coll_info[0]
            response.message.append(f"✔  Handle {cu.handle} found in Neotoma.")
            try:
                found_cu = CollectionUnit(
                    collectionunitid=int(coll_info[0]),
                    handle=str(coll_info[1]),
                    siteid=nh.clean_numbers(coll_info[2]),
                    colltypeid=nh.clean_numbers(coll_info[3]),
                    depenvtid=nh.clean_numbers(coll_info[4]),
                    collunitname=str(coll_info[5]),
                    colldate=coll_info[6],
                    colldevice=str(coll_info[7]),
                    geog=Geog(
                        (nh.clean_numbers(coll_info[8]), nh.clean_numbers(coll_info[9]))
                    ),
                    gpsaltitude=nh.clean_numbers(coll_info[10]),
                    gpserror=coll_info[11],
                    waterdepth=nh.clean_numbers(coll_info[12]),
                    substrateid=coll_info[13],
                    slopeaspect=coll_info[14],
                    slopeangle=coll_info[15],
                    location=str(coll_info[16]),
                    notes=str(coll_info[17]),
                )
            except Exception as e:
                response.valid.append(False)
                response.message.append(
                    f"✗ Cannot create Collection Unit from Neotoma Data: {e}"
                )
            updated_cu = cu.update_collunit(found_cu, overwrite, response)
            updated_cu.collectionunitid = found_cu.collectionunitid
            response.culist.append(
                {"original site": f"{cu}", "updated site": f"{updated_cu}"}
            )
            cur.execute(upsert_query)
            try:
                response.cuid = updated_cu.upsert_to_db(cur)
                response.valid.append(True)
                response.culist.append({"collunit": cu, "updated_params": updated_cu})
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"✗  Could not upsert site: {e}")
        elif len(coll_info) == 0:
            response.message.append(f"? Collunit not found")
            if overwrite["handle"] == True:
                response.valid.append(True)
                response.message.append(
                    f"✔  Overwrite is set to True."
                    f"Collection Unit is not "
                    f"currently associated to a Collection Unit in Neotoma. "
                    f"New Collection unit will be created."
                )
            else:
                response.valid.append(False)
                response.message.append(
                    f"✗  Overwrite is set to False. "
                    f"Collection Unit ID {response.cuid} is not "
                    f"currently associated to a Collection Unit in Neotoma."
                )
            response.cuid = cu.insert_to_db(cur)
            response.culist.append(cu)
    else:
        response.message.append("? Handle not given")
        cu.siteid = uploader["sites"].siteid
        try:
            cur.execute("SAVEPOINT before_try")
            response.cuid = cu.insert_to_db(cur)
            response.culist.append(cu)
            response.valid.append(True)
        except Exception as e:
            cur.execute(
                "ROLLBACK TO SAVEPOINT before_try"
            )  # Clear status of previous error.
            response.message.append(
                f"✗ Collection Unit Data is not correct." f"Error message: {e}"
            ) 
            response.cuid = cu.insert_to_db(cur)
            response.culist.append(cu)
            response.valid.append(False)
    response.validAll = all(response.valid)
    response.message.append(f"CU ID: {response.cuid}.")
    return response