import DataBUS.neotomaHelpers as nh
from DataBUS import Dataset, Response

def insert_dataset(cur, yml_dict, csv_file, uploader, name=None):
    """
    Inserts a dataset associated with a collection unit into a database.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the inserted dataset.
            'datasetid' (int): IDs for the inserted dataset.
            'valid' (bool): Indicates if insertions were successful.
    """
    response = Response()
    params = [("datasettypeid", "ndb.datasettypes.datasettypeid"),
              ("datasettype", "ndb.datasettypes.datasettype")]
    inputs = {}
    inputs['datasetname'] = nh.pull_params(['datasetname'], yml_dict, csv_file, "ndb.datasets")['datasetname']
    
    for param in params:
        val = nh.retrieve_dict(yml_dict, param[1])
        if val:
            try:
                inputs[param[0]] = val[0]['value']
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"✗ {param[0]} value is missing in template")
        else:
            inputs[param[0]] = None

    query = """SELECT datasettypeid 
               FROM ndb.datasettypes 
               WHERE LOWER(datasettype) = %(ds_type)s"""
    
    if inputs['datasettype'] and not(inputs['datasettypeid']):
        cur.execute(query, {"ds_type": f"{inputs['datasettype'].lower()}"})
        datasettypeid = cur.fetchone()
        del inputs['datasettype']

    if datasettypeid:
        inputs["datasettypeid"] = datasettypeid[0]
        response.valid.append(True)
    else:
        inputs["datasettypeid"] = None
        response.message.append(f"✗ Dataset type is not known to Neotoma and needs to be created first")
        response.valid.append(False)
    inputs["notes"] = nh.pull_params(["notes"], yml_dict, csv_file, "ndb.datasets", name)['notes']
    inputs['collectionunitid'] = uploader['collunitid'].cuid
    ds = Dataset(**inputs)
    try:
        response.datasetid = ds.insert_to_db(cur)
        response.valid.append(True)
        response.message.append(f"✔ Added Dataset {response.datasetid}.")
    except Exception as e:
        response.datasetid = ds.insert_to_db(cur)
        response.valid.append(True)
        response.message.append(
            f"✗ Cannot add Dataset {response.datasetid}." f"Using temporary ID."
        )

    response.validAll = all(response.valid)
    return response