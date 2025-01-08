import DataBUS.neotomaHelpers as nh
from DataBUS import Dataset, Response


def valid_dataset(cur, yml_dict, csv_file):
    """_Validating Datasets_"""
    response = Response()

    inputs = {
        "datasetname": nh.retrieve_dict(yml_dict, "ndb.datasets.datasetname"),
        "datasettypeid": nh.retrieve_dict(yml_dict, "ndb.datasettypes.datasettype")[
            0
        ]["value"].lower(),
        "database": nh.retrieve_dict(yml_dict, "ndb.datasetdatabases.databasename"),
    } 


    # inputs["notes"] = nh.clean_inputs(nh.pull_params(["notes"], yml_dict, csv_file, "ndb.datasets"))
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
            inputs["datasetname"] = f"EANOD/handle/OST"


    
    response.message.append(f"Datasetname: {inputs['datasetname']}")
    response.message.append(f"Datasettype: {inputs['datasettypeid']}")
    inputs["notes"] = nh.clean_inputs(
        nh.pull_params(["notes"], yml_dict, csv_file, "ndb.datasets")
    )["notes"]
    
    query = "SELECT datasettypeid FROM ndb.datasettypes WHERE LOWER(datasettype) = %(ds_type)s"
    cur.execute(query, {"ds_type": f"{inputs['datasettypeid'].lower()}"})
    datasettypeid = cur.fetchone()

    if datasettypeid:
        inputs["datasettypeid"] = datasettypeid[0]
        response.message.append("✔ Dataset type exists in neotoma.")
        response.valid.append(True)
    else:
        inputs["datasettypeid"] = None
        response.message.append(f"✗ Dataset type is not known to neotoma. Add it first")
        response.valid.append(False)

    try:
        Dataset(
            datasettypeid=inputs["datasettypeid"],
            datasetname=inputs["datasetname"],
            notes=inputs["notes"],
        )
        response.message.append(f"✔ Dataset can be created.")
        response.valid.append(True)
    except Exception as e:
        response.message.append(f"✗ Dataset cannot be created: {e}")
        response.valid.append(False)

    response.validAll = all(response.valid)

    return response