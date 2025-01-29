import DataBUS.neotomaHelpers as nh
from DataBUS import Response, Datum, Variable
import pandas as pd
import re

def insert_data_long(cur, yml_dict, filename, uploader):
    """"""
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

    for val_dict in inputs:
        var_id = []
        # data_counter = 0
        #for i in range(validator["sample"].sa_counter):
        if val_dict['value']:
            if isinstance(val_dict['value'], (int, float)) and val_dict['value'] not in {0, 1}:
                val_dict['unitcolumn'] = 'present/absent'
            else:
                val_dict['unitcolumn'] = 'present/absent'
        else:
            val_dict['unitcolumn'] = 'present/absent'

        counter = 0
        get_taxonid = (
            """SELECT taxonid FROM ndb.taxa WHERE LOWER(taxonname) = %(taxonname)s;"""
        )
        val_dict['taxonname'] = re.match(regex,val_dict['taxonname']).group(1) 
        try:
            cur.execute(get_taxonid, {"taxonname": val_dict["taxonname"].lower()})
            taxonid = cur.fetchone()
        except Exception as e:
            response.message.append(f"Could not retrieve taxon: {e}")
            response.valid.append(False)
        
        if taxonid:
            taxonid = int(taxonid[0])
        else:
            counter += 1
            taxonid = counter  # Placeholder taxon
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
    
        if not vunitsid:
            counter2 += 1
            vunitsid = counter2
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
        else:
                response.message.append(
                    f"\n? Var ID not found for: "
                    f"variableunitsid: {vunitsid[0]},\n"
                    f"taxon: {val_dict['taxonname'].lower()}, ID: {taxonid},\n"
                    f"variableelementid: {val_dict['variableelementid']},"
                    f"variablecontextid: {val_dict['variablecontextid']}"
                )
                response.valid.append(True)
                varid = var.insert_to_db(cur)
        try:
            datum = Datum(
                sampleid=int(uploader["samples"].sampleid[0]),
                variableid=int(varid),
                value=val_dict["value"],
                ) 
            response.valid.append(True)
            var_id.append(varid)
            try:
                d_id = datum.insert_to_db(cur)
                response.valid.append(True)
                response.message.append(f"✔ Added Datum {d_id}")
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"✗  Datum cannot be inserted: {e}")
                d_id = 2
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗  Datum cannot be created: {e}")

    response.validAll = all(response.valid)
    return response
