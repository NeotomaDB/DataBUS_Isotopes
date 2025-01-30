import DataBUS.neotomaHelpers as nh
from DataBUS import Sample, Response


def valid_sample(cur, yml_dict, csv_file, validator):
    """
    Validates sample data from a YAML dictionary and CSV file against a database.
    Parameters:
    cur (psycopg2.cursor): Database cursor for executing SQL queries.
    yml_dict (dict): Dictionary containing YAML data.
    csv_file (str): Path to the CSV file containing sample data.
    validator (dict): Dictionary containing validation parameters.
    Returns:
    Response: An object containing validation results, including:
        - sa_counter (int): Counter for the number of analysis units processed.
        - valid (list): List of boolean values indicating the validity of each sample.
        - message (list): List of messages indicating the validation status of each sample.
        - validAll (bool): Boolean indicating if all samples are valid.
    """
    response = Response()
    params = ["sampledate", "samplename", "analysisdate",
              "prepmethod", "notes", "taxonname"]
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.samples")
    inputs["labnumber"] = nh.retrieve_dict(yml_dict, "ndb.samples.labnumber")
    inputs["labnumber"] = inputs["labnumber"][0]["value"]

    response.sa_counter = 0
    get_taxonid = """SELECT * FROM ndb.taxa 
                     WHERE LOWER(taxonname) %% %(taxonname)s;"""
    for j in range(0, validator["analysisunit"].aucounter):
        response.sa_counter += 1
        if isinstance(inputs['taxonname'], str):
            inputs['taxonname']=inputs['taxonname'].lower()
        cur.execute(get_taxonid, {"taxonname": inputs["taxonname"]})
        taxonid = cur.fetchone()
        if taxonid != None:
            inputs['taxonid'] = int(taxonid[0])
        else:
            inputs['taxonid'] = None

        try:
            del inputs['taxonname'] 
            Sample(**inputs)
            response.valid.append(True)
        except Exception as e:
            response.message.append(f"✗ Samples data is not correct: {e}")
            response.valid.append(False)
    response.validAll = all(response.valid)
    if response.validAll:
        response.message.append(f"✔ Sample can be created.")
    return response