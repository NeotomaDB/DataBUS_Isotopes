import DataBUS.neotomaHelpers as nh
from DataBUS import Response, Contact


def insert_sample_analyst(cur, yml_dict, csv_file, uploader):
    """
    Inserts sample analyst data into Neotoma

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the inserted sample analysts.
            - 'contids' (list): List of dictionaries containing details of the analysts' IDs.
            - 'valid' (bool): Indicates if all insertions were successful.
    """
    response = Response()
    inputs = nh.pull_params(["contactid", "contactname"], yml_dict, csv_file, "ndb.sampleanalysts")
    

    if not inputs['contactid']:
        if isinstance(inputs['contactname'], list):
            seen = set()
            inputs['contactname'] = [x for x in inputs['contactname'] if not (x in seen or seen.add(x))]
            inputs['contactname'] = [value for item in inputs['contactname'] for value in item.split("|")]
            seen = set()
            inputs['contactname'] = [x for x in inputs['contactname'] if not (x in seen or seen.add(x))] # preserve order
        elif isinstance(inputs['contactname'], str):
            inputs['contactname'] = inputs['contactname'].split("|")
    else:
        inputs["contactid"] = list(dict.fromkeys(inputs["contactid"]))
    
    contids = []
    if not inputs["contactid"]:
        cont_name = nh.get_contacts(cur, inputs["contactname"])
        for i in range(len(uploader["samples"].sampleid)):
            for agent in cont_name:
                try:
                    contact = Contact(contactid=agent["id"], 
                                    order=int(agent["order"]))
                    response.valid.append(True)
                    contids.append(agent['id'])
                    try:
                        contact.insert_sample_analyst(cur, 
                                                    sampleid=int(uploader["samples"].sampleid[i]))
                        response.valid.append(True)
                        response.message.append(f"✔  Sample Analyst {agent['id']} added "
                                                f"for sample {uploader['samples'].sampleid[i]}.")
                    except:
                        response.message.append(f"Executed temporary query.")
                        response.valid.append(False)
                except Exception as e:
                    response.valid.append(False)
                    response.message.append(f"Contact cannot be created: {e}")

    response.validAll = all(response.valid)
    return response