import DataBUS.neotomaHelpers as nh
from DataBUS import Response, Datum, Variable

def valid_data(cur, yml_dict, csv_file):
    """
    Validates data from a CSV file against a YAML dictionary and a database.
    Parameters:
    cur (psycopg2.cursor): Database cursor for executing SQL queries.
    yml_dict (dict): Dictionary containing YAML configuration data.
    csv_file (str): Path to the CSV file containing data to be validated.
    validator (Validator): Validator object for additional validation logic.
    Returns:
    Response: A response object containing validation results and messages.
    """
   
    inputs = nh.pull_params(["value"], yml_dict, csv_file, "ndb.data")
    params = ["variableelement", "taxon", "variableunits", "variablecontext"]
    inputs2 = nh.pull_params(params, yml_dict, csv_file, "ndb.variables")
    inputs = {**inputs, **inputs2}

    response = Response()
    if not inputs['value']:
        response.message.append("? No Values to validate.")
        response.validAll = False
        return response

    var_query = """SELECT variableelementid FROM ndb.variableelements
                    WHERE LOWER(variableelement) = %(element)s;"""
    taxon_query = """SELECT * FROM ndb.taxa 
                     WHERE LOWER(taxonname) = %(element)s;"""
    units_query = """SELECT variableunitsid FROM ndb.variableunits 
                     WHERE LOWER(variableunits) = %(element)s;"""
    context_query = """SELECT LOWER(variablecontext) FROM ndb.variablecontexts
                       WHERE LOWER(variablecontext) = %(element)s;"""

    par = {'variableelement': [var_query, 'variableelementid'], 
           'taxon': [taxon_query, 'taxonid'], 
           'variableunits': [units_query, 'variableunitsid'],
           'variablecontext': [context_query, 'variablecontextid']}

    for i in range(len(inputs['value'])):
        entries = {}
        counter = 0
        for k,v in par.items():
            if inputs[k]:
                if inputs[k][i] != '':
                    cur.execute(v[0], {'element': inputs[k][i].lower()})
                    entries[v[1]] = cur.fetchone()
                    if not entries[v[1]]:
                        counter +=1
                        response.message.append(f"✗  {k} ID for {inputs[k][i]} not found. "
                                                f"Does it exist in Neotoma?")
                        response.valid.append(False)
                        entries[v[1]] = None
                    else:
                        entries[v[1]] = entries[v[1]][0]
                else:
                    inputs[k][i] = None
                    entries[v[1]] = None
                    response.message.append(f"?  {inputs[k][i]} ID not given. ")
                    response.valid.append(True)
            else:
                    response.message.append(f"?  {k} ID not given. ")
                    response.valid.append(True)
                    entries[v[1]] = counter
        var = Variable(**entries)
        response.valid.append(True)
        try:
            varid = var.get_id_from_db(cur)
            response.valid.append(True)
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗  Var ID cannot be retrieved from db: {e}")
            varid = None
        if varid:
            varid = varid[0]
            response.valid.append(True)
        else:
            if inputs['variablecontext']:
                varcontextid = inputs['variablecontext'][i]
            else:
                varcontextid = inputs['variablecontext']
            response.message.append(f"? Var ID not found for: \n "
                                    f"variableunitsid: {inputs['variableunits'][i]}, ID: {entries['variableunitsid']},\n"
                                    f"taxon: {inputs['taxon'][i].lower()}, ID: {entries['taxonid']},\n"
                                    f"variableelement: {inputs['variableelement'][i]}, ID: {entries['variableelementid']},\n"
                                    f"variablecontextid: {varcontextid}, ID: {entries['variablecontextid']}\n")
            response.valid.append(True)
        
        try:
            Datum(sampleid=int(3), variableid=varid, value=inputs['value'][i])
            response.valid.append(True)
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗  Datum cannot be created: {e}")
            
    response.validAll = all(response.valid)
    if response.validAll:
        response.message.append(f"✔  Datum can be created.")
    return response