import DataBUS.neotomaHelpers as nh
from DataBUS import AnalysisUnit, AUResponse

def insert_analysisunit(cur, yml_dict, csv_file, uploader):
    """_Inserting analysis units_
    Args:
        cur (_psycopg2.extensions.cursor_): _A cursor pointing to the Neotoma
            Paleoecology Database._
        yml_dict (_dict_): _A `dict` returned by the YAML template._
        csv_file (_dict_): _The csv file with the required data to be uploaded._
        uploader (_dict_): A `dict` object that contains critical information about the
          object uploaded so far.
    Returns:
        _int_: _The integer value of the newly created siteid from the Neotoma Database._
    """
    params = ["analysisunitname", "depth", "thickness",
              "faciesid", "mixed", "igsn", "notes",
              "recdatecreated", "recdatemodified"]
    try:
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.analysisunits")
    except Exception as e:
        response.validAll = False
        response.valid.append(False)
        response.message.append(f"AU Elements in the CSV file are not formatted properly. Please verify the CSV file")

    if not inputs['mixed']:
        inputs['mixed'] = False

    response = AUResponse()

    if not uploader['collunitid'].cuid:
        response.message.append(f"✗ CU ID needed to create Analysis Unit"
                                f" Placeholder `1` will be used to create log.")
        uploader['collunitid'].cuid = 1

    if inputs['depth'] and isinstance(inputs['depth'], list):
        iterable_params = {k: v for k, v in inputs.items() if isinstance(v, list)}
        static_params = {k: v for k, v in inputs.items() if not isinstance(v, list)}
        for values in zip(*iterable_params.values()):
            try:
                kwargs = dict(zip(iterable_params.keys(), values))
                kwargs.update(static_params) 
                AnalysisUnit(**kwargs)
                response.valid.append(True)
            except Exception as e:
                response.message.append(f"✗ Could not create Analysis Unit, " 
                                        f"verify entries: \n {e}")
                au = AnalysisUnit(collectionunitid=uploader["collunitid"].cuid, 
                                  mixed=False)
            try:
                auid = au.insert_to_db(cur)
                if all(response.valid) == True:
                    response.message.append(f"✔ Added Analysis Unit {auid}.")
                    response.valid.append(True)
                else:
                    response.message.append(f"✗ Analysis Unit can be created but CU has errors.")
            except Exception as e:
                response.message.append(
                    f"✗ Analysis Unit Data is not correct. Error message: {e}"
                )
                au = AnalysisUnit(collectionunitid=uploader["collunitid"].cuid)
                auid = au.insert_to_db(cur)
                response.message.append(
                    f"✗ Adding temporary Analysis Unit {auid} to continue process."
                    f"\nSite will be removed from upload."
                )
                response.valid.append(False)
            response.auid.append(auid)
    else:
        try:
            inputs['collectionunitid'] = uploader["collunitid"].cuid
            au = AnalysisUnit(**inputs)
        except Exception as e:
            response.message.append(f"✗ Could not create Analysis Unit, " 
                                    f"verify entries: \n {e}")
            au = AnalysisUnit(collectionunitid=uploader["collunitid"].cuid, 
                              mixed=False)
        try:
            auid = au.insert_to_db(cur)
            response.message.append(f"✔ Added Analysis Unit {auid}.")
            response.valid.append(True)
        except Exception as e:
            response.message.append(
                f"✗ Analysis Unit Data is not correct. Error message: {e}"
            )
            au = AnalysisUnit(collectionunitid=uploader["collunitid"].cuid)
            auid = au.insert_to_db(cur)
            response.message.append(
                f"✗ Adding temporary Analysis Unit {auid} to continue process."
                f"\nSite will be removed from upload."
            )
            response.valid.append(False)
        response.auid.append(auid)
    response.validAll = all(response.valid)
    return response