import DataBUS.neotomaHelpers as nh
from DataBUS import Response, Datum, Variable

def insert_data(cur, yml_dict, csv_file, uploader):
    """
    Inserts data into the database based on the provided YAML dictionary and CSV file.
    Parameters:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (str): Path to the CSV file containing data to be inserted.
        uploader (dict): Dictionary containing uploader information.
    Returns:
        Response: An object containing the status of the insertion process, including messages and validity of each insertion.
    """
   
    inputs = nh.pull_params(["value"], yml_dict, csv_file, "ndb.data")
    params = ["variableelement", "taxon", "variableunits", "variablecontext"]
    inputs2 = nh.pull_params(params, yml_dict, csv_file, "ndb.variables")
    inputs = {**inputs, **inputs2}

    response = Response()

    var_query = """SELECT variableelementid FROM ndb.variableelements
                    WHERE LOWER(variableelement) = %(element)s;"""
    taxon_query = """SELECT * FROM ndb.taxa 
                     WHERE LOWER(taxonname) = %(element)s;"""
    units_query = """SELECT variableunitsid FROM ndb.variableunits 
                     WHERE LOWER(variableunits) = %(element)s;"""
    #TODO variablectxt_query = """SELECT variableunitsid FROM ndb.variableunits 
    #                             WHERE LOWER(variableunits) = %(units)s;"""

    par = {'variableelement': [var_query, 'variableelementid'], 
           'taxon': [taxon_query, 'taxonid'], 
           'variableunits': [units_query, 'variableunitsid']}

    for i in range(len(inputs['value'])):
        entries = {}
        counter = 0
        for k,v in par.items():
            cur.execute(v[0], {'element': inputs[k][i].lower()})
            entries[v[1]] = cur.fetchone()
            if not entries[v[1]]:
                counter += 1
                response.message.append(f"✗  {k} ID for {inputs[k][i]} not found. "
                                        f"Does it exist in Neotoma?")
                entries[v[1]] = counter
                response.valid.append(False)
            else:
                entries[v[1]] = entries[v[1]][0]
        var = Variable(**entries)
        response.valid.append(True)
        varid = var.get_id_from_db(cur)
        if varid:
            varid = varid[0]
            response.valid.append(True)
        else:
            response.message.append(f"? Var ID not found for: "
                                    f"variableunitsid: {inputs['variableunits'][i]}, ID: {entries['variableunitsid']},\n"
                                    f"taxon: {inputs['taxon'][i].lower()}, ID: {entries['taxonid']},\n"
                                    f"variableelement: {inputs['variableelement'][i]}, ID: {entries['variableelementid']},\n"
                                    f"variablecontextid: {inputs['variablecontext']}\n")
            response.valid.append(True)
            varid = var.insert_to_db(cur)
        datum = Datum(sampleid=uploader['samples'].sampleid[0], 
                      variableid=varid, 
                      value=inputs['value'][i])
        try:
            d_id = datum.insert_to_db(cur)
            response.valid.append(True)
            response.message.append(f"✔ Added Datum {d_id}")
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗  Datum cannot be inserted: {e}")
            d_id = 2
            cur.execute("ROLLBACK;") # exit error status
            continue

    response.validAll = all(response.valid)
    if response.validAll:
        response.message.append(f"✔  Datum inserted.")
    return response