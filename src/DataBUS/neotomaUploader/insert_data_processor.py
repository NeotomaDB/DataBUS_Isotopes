from DataBUS import Contact, Response
import DataBUS.neotomaHelpers as nh


def insert_data_processor(cur, yml_dict, csv_file, uploader):
    """
    Inserts data processors into Neotoma

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the inserted data processors.
            - 'processorid' (list): List of processors' IDs.
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
    marker = False
    if not inputs["contactid"]:
        cont_name = nh.get_contacts(cur, inputs["contactname"])
        for agent in cont_name:
            try:
                contact = Contact(contactid=int(agent["id"]), 
                                    order=int(agent["order"]))
                contact.insert_data_processor(cur, 
                                                  datasetid=uploader["datasets"].datasetid)
                response.valid.append(True)
                response.message.append(f"✔ Processor {agent['id']} inserted.")
            except Exception as e:
                contact = Contact(contactid=1, order=None) #placeholder
                response.message.append(f"✗ Contact Dataset Processor is not correct. {e}")
                response.valid.append(False)
    else:
        for id in inputs["contactid"]:
            contids.append(id)
            contact = Contact(contactid=id)
            contact.insert_pi(cur,
                              collunitid=uploader["collunitid"].cuid)
            response.valid.append(True)

    response.processor = contids
    response.validAll = all(response.valid)
    return response