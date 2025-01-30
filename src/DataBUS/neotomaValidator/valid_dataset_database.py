import DataBUS.neotomaHelpers as nh
from DataBUS import Response, DatasetDatabase


def valid_dataset_database(cur, yml_dict):
    """
    Inserts dataset and database associations into Neotoma

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the dataset-database insertion.
            - 'databaseid' (int): ID of the associated database or NaN if not available.
            - 'valid' (bool): Indicates if the insertion was successful.
    """
    response = Response()

    db_name = nh.retrieve_dict(yml_dict, "ndb.datasetdatabases.databasename")
    inputs = {"databasename": db_name[0]["value"]}

    # Find databasename:
    query = """SELECT databaseid from ndb.constituentdatabases
                WHERE LOWER(databasename) LIKE %(databasename)s"""
    cur.execute(query, {"databasename": f"%{inputs['databasename'].lower()}%"})
    inputs["databaseid"] = cur.fetchone()
    if inputs["databaseid"]:
        inputs["databaseid"] = inputs["databaseid"][0]
    try:
        DatasetDatabase(databaseid=int(inputs["databaseid"]))
        response.valid.append(True)
        response.message.append(f"✔ Database ID {inputs['databaseid']} " f"created.")

    except Exception as e:
        response.message.append(f"✗ Cannot create Database object: {e}")
        response.valid.append(False)

    response.databaseid = inputs["databaseid"]
    response.validAll = all(response.valid)
    return response
