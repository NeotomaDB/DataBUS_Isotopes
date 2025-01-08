import DataBUS.neotomaHelpers as nh
from DataBUS import Publication, Response
import requests
import re

def insert_publication(cur, yml_dict, csv_file, uploader):
    """
    Inserts publication data into Neotoma.

    
    """
    response = Response()

    params = [
        "doi", "publicationid", "citation"
    ]
    inputs = nh.clean_inputs(
        nh.pull_params(params, yml_dict, csv_file, "ndb.publications")
    )

    if inputs["publicationid"]:
        inputs["publicationid"] = [
            value if value != "NA" else None for value in inputs["publicationid"]
        ]
        inputs["publicationid"] = inputs["publicationid"][0]
    doi_pattern = r"^10\.\d{4,9}/[-._;()/:A-Z0-9]+$"

    dataset_pub_q = """SELECT ts.insertdatasetpublication(%(datasetid)s, 
                                                          %(publicationid)s, 
                                                          %(primarypub)s)
                                                          """
    #inputs['doi'][0] = "10.1038/nature12373" #placeholder for trial.
    if inputs['publicationid'] is None:
        response.message.append(f"? No DOI present")
        response.valid.append(True)
        if inputs['citation']:
            for cit in inputs['citation']:
                cit_q = """
                        SELECT *
                        FROM ndb.publications
                        WHERE citation IS NOT NULL
                        ORDER BY similarity(LOWER(citation), %(cit)s) DESC
                        LIMIT 1; """
                cur.execute(cit_q, {'cit': cit.lower()})
                obs = cur.fetchall()
                if obs:
                    pub_id = obs[0]
                    response.message.append(f"✔  Found Publication: "
                                            f"{pub_id[3]} in Neotoma")
                    response.valid.append(True)
                    try: 
                        cur.execute(dataset_pub_q, {'datasetid': uploader["datasetid"].datasetid,
                                                    'publicationid': pub_id[0],
                                                    'primarypub': True})
                        response.valid.append(True)
                    except Exception as e:
                        response.message.append("Could not associate dataset ID to publication ID")
                        response.valid.append(False)
                else:
                    response.message.append("✗  The publication does not exist in Neotoma.")
                    response.valid.append(False)
    else:
        try:
            inputs['publicationid'] = int(inputs['publicationid'])
        except Exception as e:
            response.message.append("Cannot coerce publication ID to integer. Try as DOI.")
        if isinstance(inputs['publicationid'], int):
            pub_query = """
                        SELECT * FROM ndb.publications
                        WHERE publicationid = %(pubid)s
                        """
            cur.execute(pub_query, {'pubid': inputs['publicationid']})
            pub = cur.fetchone()
            if pub:
                response.message.append(f"✔  Found Publication: "
                                        f"{pub[3]} in Neotoma")
                response.valid.append(True)
                cur.execute(dataset_pub_q, {'datasetid': uploader["datasetid"].datasetid,
                                            'publicationid': inputs['publicationid'],
                                            'primarypub': True})
            else:
                response.message.append("✗  The publication does not exist in Neotoma.")
                response.valid.append(False)
        elif isinstance(inputs['publicationid'], str):
            if re.match(doi_pattern, inputs['publicationid'], re.IGNORECASE):
                response.message.append(f"✔  Reference is correctly formatted as DOI.")
                response.valid.append(True)
                url = f"https://api.crossref.org/works/{inputs['doi'][0]}"
                request = requests.get(url)
                if request.status_code == 200:
                    response.message.append(f"✔  DOI {inputs['doi']} found in CrossRef")
                    response.valid.append(True)
                    data = request.json()
                    data = data['message']
                else:
                    response.message.append(f"✗  No DOI {inputs['doi']} found in CrossRef")
                    data = None
                try:
                    pub_type = data['type']
                    sql_neotoma = """SELECT pubtypeid FROM ndb.publicationtypes
                                    WHERE LOWER(REPLACE(pubtype, ' ', '-')) LIKE %(pub_type)s
                                    LIMIT 1"""
                    cur.execute(sql_neotoma, {'pub_type': pub_type.lower()})
                    pubtypeid = cur.fetchone()
                    if pubtypeid:
                        pubtypeid = pubtypeid[0]

                    pub = Publication(pubtypeid = pubtypeid,
                                    year = None,
                                    citation = None,
                                    title = data['title'][0],
                                    journal = data['container-title'][0],
                                    vol = data['volume'],
                                    issue = data['journal-issue']['issue'],
                                    pages = data['page'],
                                    citnumber = str(data['is-referenced-by-count']),
                                    doi = data['DOI'],
                                    booktitle = None,
                                    numvol = data['volume'],
                                    edition = None,
                                    voltitle = None,
                                    sertitle = None,
                                    servol = None,
                                    publisher = data['publisher'],
                                    url = data['URL'],
                                    city = None,
                                    state = None,
                                    country = None,
                                    origlang = data['language'],
                                    notes = None)
                    response.valid.append(True)
                except Exception as e:
                    response.valid.append(False)
                    response.message.append("✗  Publication cannot be created {e}")
                    #pub = Publication()
                try:
                    pubid = pub.insert_to_db(cur)
                    response.pub = pubid
                    response.valid.append(True)
                    response.message.append(f"✔ Added Publication {pubid}.")

                except Exception as e:
                    response.message.append(
                        f"✗  Publication Data is not correct. Error message: {e}"
                    )
                    pub = Publication()
                    pubid = pub.insert_to_db(cur)
                    response.valid.append(False)
                finally:
                    try:
                        dataset_pub_q = """SELECT ts.insertdatasetpublication(%(datasetid)s, 
                                                                            %(publicationid)s, 
                                                                            %(primarypub)s)
                                        """
                        cur.execute(dataset_pub_q, {'datasetid': uploader["datasetid"].datasetid,
                                                    'publicationid': pubid,
                                                    'primarypub': True})
                    except Exception as e:
                        response.message.append("Could not associate dataset ID to publication ID")
                        response.valid.append(False)
            else:
                response.message.append("? Text found in reference column but "
                                        f"it does not meet DOI standards."
                                        f"Publication info will not be uploaded.")
    response.validAll = all(response.valid)
    return response