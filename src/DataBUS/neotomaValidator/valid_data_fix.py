import DataBUS.neotomaHelpers as nh
from DataBUS import Response, Datum, Variable


def valid_data(cur, yml_dict, csv_file, validator):
    """"""
    response = Response()

    params = ["value"]
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.data")
    uncertainty_d = []

    params2 = ["variableelementid", "variablecontextid"]
    inputs2 = nh.clean_inputs(nh.pull_params(params2, yml_dict, csv_file, "ndb.data"))
    inputs2 = {
        p: [None] * validator["sample"].sa_counter if inputs2[p] is None else inputs2[p]
        for p in params2
    }

    for val_dict in inputs:  # for sample
        data_counter = 0
        for i in range(validator["sample"].sa_counter):
            counter = 0
            get_taxonid = (
                """SELECT * FROM ndb.taxa WHERE LOWER(taxonname) = %(taxonname)s;"""
            )
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

            val_dict["value"] = [
                None if item == "NA" else item for item in val_dict["value"]
            ]

            # Get UnitsID
            get_vunitsid = """SELECT variableunitsid FROM ndb.variableunits 
                                WHERE LOWER(variableunits) = %(units)s;"""
            cur.execute(get_vunitsid, {"units": val_dict["unitcolumn"][i].lower()})
            vunitsid = cur.fetchone()  # This is to get varunitsid
            counter2 = 0
            #if vunitsid:
            #    response.message.append(f"✔ Units ID {vunitsid} found.")
            if not vunitsid:
                counter2 += 1
                vunitsid = counter
                response.message.append(
                    f"✗  UnitsID for {val_dict['unitcolumn'][i].lower()} "
                    f"not found. \nDoes it exist in Neotoma?"
                    f"Temporary UnitsID {vunitsid} for insert."
                )
                response.valid.append(False)

            try:
                var = Variable(
                    variableunitsid=vunitsid,
                    taxonid=taxonid,
                    variableelementid=inputs2["variableelementid"][i],
                    variablecontextid=inputs2["variablecontextid"][i],
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
                try:
                    varid = var.insert_to_db(cur)
                    response.message.append(
                        f"? Var ID not found for: "
                        f"variableunitsid: {vunitsid[0]},\n"
                        f"taxon: {val_dict['taxonname'].lower()}, ID: {taxonid},\n"
                        f"variableelementid: {inputs2['variableelementid'][i]},"
                        f"variablecontextid: {inputs2['variablecontextid'][i]}\n"
                        f"ts.insertvariable new ID: {varid}"
                    )
                    response.valid.append(True)
                except Exception as e:
                    response.valid.append(False)
                    response.message.append(f"✗ Cannot insert Var ID")
                    varid = 1  # error placeholder

            #### Where the datum stuff begins

            try:
                # variableID is needed in Neotoma - we have to create
                # in validation so that we can guarantee that the
                # information to create Datum objects is possible.
                Datum(
                    sampleid=int(i), variableid=int(varid), value=val_dict["value"][i]
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
