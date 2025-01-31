import DataBUS.neotomaHelpers as nh
from DataBUS import Sample, Response


def insert_sample(cur, yml_dict, csv_file, uploader):
    """
    Inserts sample data into Neotoma.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the inserted samples.
            - 'samples' (list): List of sample IDs inserted into the database.
            - 'valid' (bool): Indicates if all insertions were successful.
    """
    response = Response()
    params = ["sampledate", "samplename", "analysisdate",
              "prepmethod", "notes", "taxonname"]
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.samples")
    inputs["labnumber"] = nh.retrieve_dict(yml_dict, "ndb.samples.labnumber")
    inputs["labnumber"] = inputs["labnumber"][0]["value"]

    get_taxonid = """SELECT * FROM ndb.taxa WHERE taxonname %% %(taxonname)s;"""
    for j in range(len(uploader["anunits"].auid)):
        cur.execute(get_taxonid, {"taxonname": inputs["taxonname"]})
        taxonid = cur.fetchone()
        if taxonid:
            inputs['taxonid'] = int(taxonid[0])
        else:
            inputs['taxonid'] = None
        try:
            inputs.pop('taxonname', None)  
            inputs['analysisunitid'] = uploader["anunits"].auid[j]
            inputs['datasetid'] = uploader["datasets"].datasetid
            sample = Sample(**inputs)
            response.valid.append(True)
            try:
                s_id = sample.insert_to_db(cur)
                response.sampleid.append(s_id)
                response.valid.append(True)
                response.message.append(f"✔  Added Sample {s_id}.")
            except Exception as e:
                s_id = sample.insert_to_db(cur)
                response.sampleid.append(s_id)
                response.valid.append(True)
                response.message.append(f"✗  Cannot add sample: {e}.")
        except Exception as e:
            sample = Sample()
            response.message.append(f"✗ Samples data is not correct: {e}")
            response.valid.append(False)

    if not len(uploader["anunits"].auid) == len(response.sampleid):
        response.message.append("✗  Analysis Units and Samples do not have same length.")
    response.validAll = all(response.valid)
    return response