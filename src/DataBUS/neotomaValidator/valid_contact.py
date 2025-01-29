import DataBUS.neotomaHelpers as nh
from DataBUS import Contact, Response

def valid_contact(cur, csv_file, yml_dict):
    """
    Validate contact information against the Neotoma Paleoecology Database.

    This function checks the provided contact information (either contact IDs or contact names) 
    against the Neotoma Paleoecology Database to ensure they exist and are valid. It processes 
    the input data, queries the database for matches, and returns a response object containing 
    validation results and messages.
    
    Parameters:
        cur (psycopg2.extensions.cursor): A cursor pointing to the Neotoma Paleoecology Database.
        csv_file (str): A user name or individual.
        yml_dict (dict): The dictionary object passed by template_to_dict.

    Returns:
        Response: An object containing validation results and messages.
    """
    response = Response()

    params = ["contactid", "contactname"]
    table = ["ndb.datasetpis", "ndb.sampleanalysts", "ndb.collectors", "ndb.chronologies", "ndb.datasetprocessor"]
    inputs = nh.pull_params(params, yml_dict, csv_file, table)

    for i, id in enumerate(inputs):
        if not id['contactid']:
            if isinstance(id['contactname'], list):
                id['contactname'] = list(set(id["contactname"]))
                id['contactname'] = [value for item in id['contactname'] for value in item.split("|")]
                id['contactname'] = list(set(id["contactname"]))
            elif isinstance(id['contactname'], str):
                id['contactname'] = id['contactname'].split("|")
        else:
            id["contactid"] = list(set(id["contactid"]))
        id["table"] = table[i]

    for element in inputs:
        response.message.append(f"  === Checking Against Database - Table: {element['table']}.contactid ===")
        agentname = element["contactname"]
        agentid = element["contactid"]
        if not agentid:
            response.message.append(f"? Contact ID not given.")
            if not agentname:
                response.message.append(f"? Contact Name not given.")
                continue
            else:
                namematch = []
                for name in agentname:
                    response.message.append(f"  *** Named Contact: {name} ***")
                    nameQuery = """SELECT contactid, contactname, similarity(contactname, %(name)s) AS sim_score
                                FROM ndb.contacts
                                WHERE LOWER(contactname) %% %(name)s
                                ORDER BY sim_score DESC
                                LIMIT 3;"""
                    cur.execute(nameQuery, {"name": name.lower()})
                    result = {"name": name, "match": cur.fetchall()}
                    namematch.append(result)
        for person in namematch:
            if len(person["match"]) == 0:
                response.message.append(f"  ✗ No approximate matches found for "
                                        f"{person['name']}. Have they been added to Neotoma?")
                response.valid.append(False)
            elif any([person["name"] == i[1] for i in person["match"]]):
                id = next(
                    (number for number, name, sim in person["match"]
                     if name == person["name"]),None)
                response.message.append(f"  ✔ Exact match found for {person['name']}.")
                response.valid.append("True")
                try:
                    Contact(contactid=id, contactname=person["name"], order=None)
                    response.valid.append(True)
                except Exception as e:
                    response.valid.append(False)
                    response.message.append(f"  ✗ Cannot create Contact object: {e}")
            else:
                response.message.append(
                    f"  ? No exact match found for {person['name']}, but several potential matches follow:"
                )
                for i in person["match"]:
                    response.message.append(f"   * {i[1]}")
    response.validAll = all(response.valid)
    return response