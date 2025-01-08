import DataBUS.neotomaHelpers as nh
from DataBUS import Dataset, Response

def insert_dataset(cur, yml_dict, csv_file, uploader):
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
    inputs = {
        "datasetname": nh.retrieve_dict(yml_dict, "ndb.datasets.datasetname"),
        "datasettypeid": nh.retrieve_dict(yml_dict, "ndb.datasettypes.datasettype")[
            0
        ]["value"].lower(),
        "database": nh.retrieve_dict(yml_dict, "ndb.datasetdatabases.databasename"),
    } 

    if inputs["datasetname"] and isinstance(inputs["datasetname"], list):
        if isinstance(inputs["datasetname"][0], dict):
            if isinstance(inputs["datasetname"][0]["value"], str):
                inputs["datasetname"] = inputs["datasetname"][0]["value"].lower()
            else:
                inputs["datasetname"] = inputs["datasetname"][0]["value"]
    else:
        inputs["datasetname"] = None

    if inputs["datasetname"] == None:
        if inputs["database"][0]['value'].lower() == "East Asian Nonmarine Ostracod Database".lower():
            inputs["datasetname"] = f"EANOD/{uploader['collunitid'].handle}/OST"
        #Extract database column

    inputs["notes"] = nh.clean_inputs(
        nh.pull_params(["notes"], yml_dict, csv_file, "ndb.datasets")
    )["notes"]

    query = "SELECT datasettypeid FROM ndb.datasettypes WHERE LOWER(datasettype) = %(ds_type)s"
    cur.execute(query, {"ds_type": f"{inputs['datasettypeid'].lower()}"})
    inputs["datasettypeid"] = cur.fetchone()

    if inputs["datasettypeid"]:
        inputs["datasettypeid"] = inputs["datasettypeid"][0]

    try:
        ds = Dataset(
            datasettypeid=inputs["datasettypeid"],
            collectionunitid=uploader["collunitid"].cuid,
            datasetname=inputs["datasetname"],
            notes=inputs["notes"],
        )
        response.valid.append(True)
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗ Dataset was not created: {e}"
                                f"Placeholder `10` will be used.")
        ds = Dataset(
            datasettypeid=10,
            collectionunitid=uploader["collunitid"].cuid,
        )
    finally:
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