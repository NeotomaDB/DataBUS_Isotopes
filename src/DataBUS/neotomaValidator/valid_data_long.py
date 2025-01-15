import DataBUS.neotomaHelpers as nh
from DataBUS import Response, Datum, Variable
import pandas as pd
import re

def valid_data_long(cur, yml_dict, csv_file, validator, filename):
    """"""
    uncertainty_d = []
    response = Response()
    df = pd.read_csv(filename)
    df = df.rename(columns={'NAME IN REFERENCE': 'scientificName',
                       'NO SPEC': 'organismQuantity'})
    columns_to_check = ['scientificName', 'organismQuantity', 'variableelementid', 'variablecontextid']
    existing_columns = [col for col in columns_to_check if col in df.columns]

    var_element = nh.retrieve_dict(yml_dict, "ndb.variableelements.variableelementid")
    var_element = var_element[0]['value']
    var_query = """SELECT variableelementid FROM ndb.variableelements
                    WHERE LOWER(variableelement) = %(var_element)s;"""
    cur.execute(var_query, {'var_element': var_element})
    var_id = cur.fetchone()

    inputs = [{'taxonname': row['scientificName'], 
               'value': None if pd.isna(row['organismQuantity']) 
               else row['organismQuantity'],
               'variableelementid': var_id[0] if var_id else None,
               'variablecontextid': row['variablecontextid'] if 'variablecontextid' in existing_columns else None} for _, row in df.iterrows()]
    
    regex = r'^(\w+\s*\w+)'

    for i, val_dict in enumerate(inputs):
        if val_dict['value']:
            if isinstance(val_dict['value'], (int, float)) and val_dict['value'] not in {0, 1}:
                val_dict['unitcolumn'] = 'NISP'
            else:
                val_dict['unitcolumn'] = 'present/absent'
        else:
            val_dict['unitcolumn'] = 'present/absent'

        counter = 0
        get_taxonid = (
            """SELECT * FROM ndb.taxa WHERE LOWER(taxonname) = %(taxonname)s;"""
        )
        val_dict['taxonname'] = re.match(regex,val_dict['taxonname']).group(1) 
    
        cur.execute(get_taxonid, {"taxonname": val_dict["taxonname"].lower()})
        taxonid = cur.fetchone()

        if taxonid:
            taxonid = int(taxonid[0])
            #response.message.append(f"✔ Taxon ID {taxonid} found.")
        else:
            counter += 1
            taxonid = counter  # To do temporary taxon
            response.message.append(
                f"✗  Taxon ID for {val_dict['taxonname']} not found."
                f"Does it exist in Neotoma?"
            )
            response.valid.append(False)

        # Get UnitsID
        get_vunitsid = """SELECT variableunitsid FROM ndb.variableunits 
                            WHERE LOWER(variableunits) = %(units)s;"""
        cur.execute(get_vunitsid, {"units": val_dict["unitcolumn"].lower()})
        vunitsid = cur.fetchone()  # This is to get varunitsid
        counter2 = 0
        #if vunitsid:
        #    response.message.append(f"✔ Units ID {vunitsid} found.")
        if not vunitsid:
            counter2 += 1
            vunitsid = counter
            response.message.append(
                f"✗  UnitsID for {val_dict['unitcolumn'].lower()} "
                f"not found. \nDoes it exist in Neotoma?"
                f"Temporary UnitsID {vunitsid} for insert."
            )
            response.valid.append(False)

        try:
            var = Variable(
                variableunitsid=vunitsid,
                taxonid=taxonid,
                variableelementid=None,
                variablecontextid=None,
            )
            response.valid.append(True)
        except Exception as e:
            var = Variable(
                variableunitsid=vunitsid,
                taxonid=taxonid,
                variableelementid=None,
                variablecontextid=None,
            )
            response.valid.append(False)
            response.message.append(f"✗  Variable cannot be created: {e}")
        finally:
            try:
                varid = var.get_id_from_db(cur)
                response.valid.append(True)
            except Exception as e:
                response.valid.append(False)
                response.message.append(
                    f"✗  Var ID cannot be retrieved from db: {e}"
                )
                varid = None
        if varid:
            varid = varid[0]
            response.valid.append(True)
            #response.message.append(f"✔ Var ID {varid} found.")
        else:
                response.message.append(
                    f"? Var ID not found for: "
                    f"variableunitsid: {vunitsid[0]},\n"
                    f"taxon: {val_dict['taxonname'].lower()}, ID: {taxonid},\n"
                    f"variableelementid: {val_dict['variableelementid']},"
                    f"variablecontextid: {val_dict['variablecontextid']}\n"
                ) 
                response.valid.append(True)

        #### Where the datum stuff begins

        try:
            Datum(
                sampleid=int(i), variableid=varid, value=val_dict["value"]
            )
            response.valid.append(True)
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗  Datum cannot be created: {e}")
        finally:
            if "uncertainty" in val_dict:
                data_counter += 1
        if "uncertainty" in val_dict:
            uncertainty_d.append(data_counter)

    response.validAll = all(response.valid)
    response.uncertainty_inputs = uncertainty_d

    if response.validAll:
        response.message.append(f"✔  Datum can be created.")

    return response
