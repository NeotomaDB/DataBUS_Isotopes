from DataBUS import Response, Contact
import DataBUS.neotomaHelpers as nh


def insert_dataset_pi(cur, yml_dict, csv_file, uploader):
    """
    Inserts dataset principal investigator data into Neotomas.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the inserted dataset principal investigators.
            - 'dataset_pi_ids' (list): List of dictionaries containing details of the contacts, including their IDs and order.
            - 'valid' (bool): Indicates if all insertions were successful.
    """
    response = Response()
    inputs = nh.pull_params(["contactid", "contactname"], yml_dict, csv_file, "ndb.datasetpis")

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
    marker = False
    if not inputs["contactid"]:
        cont_name = nh.get_contacts(cur, inputs["contactname"])
        for agent in cont_name:
            try:
                contact = Contact(contactid=int(agent["id"]), 
                                    order=int(agent["order"]))
                response.valid.append(True)
                marker = True
            except Exception as e:
                contact = Contact(contactid=None, order=None)
                response.message.append(f"✗ Contact DatasetPI is not correct. {e}")
                response.valid.append(False)
            if marker == True:
                try:
                    contact.insert_pi(cur, uploader["datasets"].datasetid)
                    response.message.append(f"✔ Added PI {agent['id']}.")
                    contids.append(agent['id'])
                except Exception as e:
                    response.message.append(f"✗ DatasetPI cannot be added. {e}")
                    response.valid.append(False)
            else:
                response.message.append(f"✗ Data PI information is not correct.")
                response.valid.append(False)
    else:
        for id in inputs["contactid"]:
            contids.append(id)
            contact = Contact(contactid=id)
            contact.insert_pi(cur,
                              collunitid=uploader["collunitid"].cuid)
            response.valid.append(True)

    response.datasetpi = contids
    response.validAll = all(response.valid)
    return response