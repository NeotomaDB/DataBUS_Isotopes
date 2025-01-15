import DataBUS.neotomaHelpers as nh
from DataBUS import AnalysisUnit, AUResponse

def valid_analysisunit(yml_dict, csv_file):
    """_Inserting analysis units_"""
    params = [
        "analysisunitname",
        "depth",
        "thickness",
        "faciesid",
        "mixed",
        "igsn",
        "notes",
        "recdatecreated",
        "recdatemodified",
    ]
    
    response = AUResponse()
    try:
        inputs = nh.clean_inputs(
            nh.pull_params(params, yml_dict, csv_file, "ndb.analysisunits")
        )
    except Exception as e:
        response.validAll = False
        response.message.append(f"AU Elements in the CSV file are not properly inserted. Please verify the CSV file")

    for k in inputs:
        if inputs[k] is None:
            response.message.append(f"? {k} has no values.")
            response.valid.append(True)
        else:
            response.message.append(f"✔ {k} has values.")
            response.valid.append(True)

    if inputs["depth"] and len(inputs["depth"]) > 0:
        response.aucounter = 0
        for i in range(0, len(inputs["depth"])):
            try:
                AnalysisUnit(
                    analysisunitid=None,
                    collectionunitid=None,
                    analysisunitname=inputs["analysisunitname"],
                    depth=inputs["depth"][i],
                    thickness=inputs["thickness"][i],
                    faciesid=inputs["faciesid"],  # verify if it is row by row
                    mixed=inputs["mixed"],  # verify if it is row by row
                    igsn=inputs["igsn"],
                    notes=inputs["notes"],
                    recdatecreated=inputs["recdatecreated"],
                    recdatemodified=inputs["recdatemodified"],
                )
                response.valid.append(True)
            except Exception as e:  # for now
                response.valid.append(False)
                response.message.append(f"✗ AnalysisUnit cannot be created: " 
                                        f"{e}")
            response.aucounter += 1
    else:
        AnalysisUnit(analysisunitid=None,
                    collectionunitid=None,
                    analysisunitname=inputs["analysisunitname"],
                    depth=inputs["depth"],
                    thickness=inputs["thickness"],
                    faciesid=inputs["faciesid"], 
                    mixed=inputs["mixed"],
                    igsn=inputs["igsn"],
                    notes=inputs["notes"],
                    recdatecreated=inputs["recdatecreated"],
                    recdatemodified=inputs["recdatemodified"])
        response.aucounter = 1
        
    response.message = list(set(response.message))
    response.validAll = all(response.valid)
    if response.validAll:
        response.message.append("✔ AnalysisUnit can be created")
    return response
