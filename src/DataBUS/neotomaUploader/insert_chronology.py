import DataBUS.neotomaHelpers as nh
from DataBUS import Chronology, ChronResponse
from datetime import datetime

def insert_chronology(cur, yml_dict, csv_file, uploader):
    """
    Inserts chronology data into Neotoma.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): Dictionary containing information about the inserted chronology.
        Contains keys:
            'chronology': ID of the inserted chronology.
            'valid': Boolean indicating if the insertion was successful.
    """
    response = ChronResponse()

    params = ["chronologyid", "collectionunitid", "contactid",
              "isdefault", "chronologyname", "dateprepared", 
              "agemodel", "ageboundyounger", "ageboundolder", 
              "notes", "recdatecreated", "recdatemodified",
              "age", "agetype"]

    try:
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.chronologies")
    except Exception as e:
        error_message = str(e)
        try:
            if "time data" in error_message.lower():
                age_dict = nh.retrieve_dict(yml_dict, "ndb.chronologies.age")
                column = age_dict[0]['column']
                if isinstance(csv_file[0][column], str) and len(csv_file[0][column]) >= 4:
                    if len(csv_file[0][column]) == 7 and csv_file[0][column][4] == '-' and csv_file[0][column][5:7].isdigit():
                        new_date = f"{csv_file[0][column]}-01"
                        new_date = new_date.replace('-', '/')
                        new_date = datetime.strptime(new_date, "%Y/%m/%d")
                    elif csv_file[0][column][:4].isdigit():
                        new_date = int(csv_file[0][column][:4])
                    else:
                        new_date = None
                else:
                    new_date = None
            params.remove('age')
            inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.chronologies")
            inputs['age'] = new_date
            response.valid.append(True)
        except Exception as inner_e:
            response.validAll = False
            response.message.append("Chronology parameters cannot be properly extracted. {e}\n")
            response.message.append(str(inner_e))
            return response

    if inputs['agemodel'] == "collection date":
        if isinstance(inputs['age'], (float, int)):
            inputs['age'] = 1950 - inputs['age']
        elif isinstance(inputs['age'], datetime):
            inputs['age'] = 1950 - inputs['age'].year
        elif isinstance(inputs['age'], list):
            inputs['age'] = [1950 - value.year if isinstance(value, datetime) else 1950 - value
                             for value in inputs['age']]
            if not (inputs["ageboundolder"] and inputs["ageboundyounger"]):
                inputs["ageboundyounger"]= int(min(inputs["age"])) 
                inputs["ageboundolder"]= int(max(inputs["age"])) 
    
    # to add for lead models because they use more calendar format
    
    if inputs["agetype"]: 
        inputs["agetype"]=inputs["agetype"].replace("collection date", 'Calendar years BP')
        if not inputs['chronologyname']:
            inputs["chronologyname"] = inputs["agetype"]
        agetype_query = """SELECT agetypeid FROM ndb.agetypes
                           WHERE LOWER(agetype) = %(agetype)s"""
        cur.execute(agetype_query, {'agetype': inputs["agetype"].lower()})
        id = cur.fetchone()
        if id:
            inputs["agetypeid"] = id[0]
            response.message.append("✔ The provided age type is correct.")
            response.valid.append(True)
        else:
            response.message.append("✗ The provided age type does not exist in Neotoma DB.")
            response.valid.append(False)
            inputs["agetypeid"] = None
    else:
        response.message.append("? No age type provided.")
        response.valid.append(True)
        inputs["agetypeid"] = None

    del inputs["agetype"], inputs["age"]

    inputs['collectionunitid']=uploader["collunitid"].cuid
    chron= Chronology(**inputs)
    
    #  to add for lead models because they use more calendar format
    try:
        chronid = chron.insert_to_db(cur)
        response.chronid = chronid
        response.valid.append(True)
        response.message.append(f"✔ Added Chronology {chronid}.")
    except Exception as e:
        response.message.append(f"✗  Chronology Data is not correct. "
                                f"Error message: {e}")
        chron = Chronology(collectionunitid=uploader["collunitid"].cuid, agetypeid=1)
        chronid = chron.insert_to_db(cur)
        response.valid.append(False)
    response.validAll = all(response.valid)
    return response