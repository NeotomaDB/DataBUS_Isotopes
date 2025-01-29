import DataBUS.neotomaHelpers as nh
from DataBUS import AnalysisUnit, AUResponse

def valid_analysisunit(yml_dict, csv_file):
    """_Inserting analysis units_"""
    params = ["analysisunitname", "depth", "thickness",
              "faciesid", "mixed", "igsn", "notes",
              "recdatecreated", "recdatemodified"]
    response = AUResponse()

    try:
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.analysisunits")
    except Exception as e:
        response.validAll = False
        response.message.append(f"AU Elements in the CSV file are not properly inserted. Please verify the CSV file")
    inputs['analysisunitid']=None
    inputs['collectionunitid']=None

    for k in inputs:
        if inputs[k] is None:
            response.message.append(f"? {k} has no values.")
            response.valid.append(True)
        else:
            response.message.append(f"✔ {k} has values.")
            response.valid.append(True)
    if inputs["depth"] and isinstance(inputs['depth'], list):
        response.aucounter = 0
        iterable_params = {k: v for k, v in inputs.items() if isinstance(v, list)}
        static_params = {k: v for k, v in inputs.items() if not isinstance(v, list)}
        for values in zip(*iterable_params.values()):  # Loops over the lists
            try:
                kwargs = dict(zip(iterable_params.keys(), values))  # Create dictionary with lists
                kwargs.update(static_params) 
                AnalysisUnit(**kwargs)
                response.valid.append(True)
            except Exception as e:  # for now
                response.valid.append(False)
                response.message.append(f"✗ AnalysisUnit cannot be created: " 
                                        f"{e}")
            response.aucounter += 1
    else:
        AnalysisUnit(**inputs)
        response.aucounter = 1
        
    response.message = list(set(response.message))
    response.validAll = all(response.valid)
    if response.validAll:
        response.message.append("✔ AnalysisUnit can be created")
    return response