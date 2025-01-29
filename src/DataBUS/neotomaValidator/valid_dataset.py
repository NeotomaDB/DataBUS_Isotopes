import DataBUS.neotomaHelpers as nh
from DataBUS import Dataset, Response

def valid_dataset(cur, yml_dict, csv_file, name=None):
    """
    Validates a dataset based on provided YAML dictionary and CSV file.

    Args:
        cur (cursor): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): Path to the CSV file.
        name (str, optional): Name of the dataset. Defaults to None.

    Returns:
        Response: An object containing validation results, including messages and validity status.

    Raises:
        Exception: If there are issues with retrieving values from the YAML dictionary or creating the dataset.
"""
    response = Response()

    params = [("datasetname", "ndb.datasets.datasetname"),
              ("datasettypeid", "ndb.datasettypes.datasettypeid"),
              ("datasettype", "ndb.datasettypes.datasettype")]
    inputs = {}
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
    inputs["notes"] = nh.pull_params(["notes"], yml_dict, csv_file, "ndb.datasets", name)
    try:
        Dataset(**inputs)
        response.message.append(f"✔ Dataset can be created.")
        response.valid.append(True)
    except Exception as e:
        response.message.append(f"✗ Dataset cannot be created: {e}")
        response.valid.append(False)

    response.validAll = all(response.valid)
    return response