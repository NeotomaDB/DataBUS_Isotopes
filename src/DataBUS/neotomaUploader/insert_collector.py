from DataBUS import Contact, Response
import DataBUS.neotomaHelpers as nh

def insert_collector(cur, yml_dict, csv_file, uploader):
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
    inputs = nh.pull_params(["contactid", "contactname"], yml_dict, csv_file, "ndb.collectors")

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
        for agent in cont_name:
            try: 
                contact = Contact(contactid=agent["id"])
                response.valid.append(True)
                contids.append(agent['id'])
                try:
                    contact.insert_collector(cur, 
                                             collunitid=uploader["collunitid"].cuid)
                    response.valid.append(True)
                    response.message.append(f"✔ Collector {agent['id']} inserted.")
                except Exception as e:
                    response.message.append(f"✗ Data collector information is not correct. {e}")
                    response.valid.append(False)
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"Cannot create Collector: {e}")
                
    else:
        for id in inputs["contactid"]:
            contids.append(id)
            contact = Contact(contactid=id)
            contact.insert_collector(cur,
                                     collunitid=uploader["collunitid"].cuid)
            response.valid.append(True)

    response.collector = contids
    response.validAll = all(response.valid)
    return response