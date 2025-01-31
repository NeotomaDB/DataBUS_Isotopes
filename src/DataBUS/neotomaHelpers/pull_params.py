import datetime
import re
from .retrieve_dict import retrieve_dict
from .clean_column import clean_column
from .clean_notes import clean_notes

def pull_params(params, yml_dict, csv_template, table=None, name = None):
    """
    Pull parameters associated with an insert statement from the yml/csv tables.

    Args:
        params (_list_): A list of strings for the columns needed to generate the insert statement.
        yml_dict (_dict_): A `dict` returned by the YAML template.
        csv_template (_dict_): The csv file with the required data to be uploaded.
        table (_string_): The name of the table the parameters are being drawn for.

    Returns:
        _dict_: Cleaned and repeated valors for input into a ts.insert functions.
    """
    results = []
    if isinstance(table, str):
        add_unit_inputs = {}
        if re.match(".*\.$", table) == None:
            table = table + "."
        add_units_inputs_list = []
        for param in params:
            subfields = [entry for entry in yml_dict['metadata'] if entry.get('neotoma', '').startswith(f'{table}{param}.')]
            if subfields:
                for entry in subfields:
                    param_name = entry['neotoma'].replace(f'{table}', "")
                    params.append(param_name)
                params.remove(param)
        for i in params:
            valor = retrieve_dict(yml_dict, table + i)
            if len(valor) > 0:
                for count, val in enumerate(valor):
                    clean_valor = clean_column(val.get("column"), 
                                               csv_template, 
                                               clean=not val.get("rowwise"))
                    if clean_valor:
                        match val.get("type"):
                            case "date":
                                if val.get("rowwise"):
                                    clean_valor = list(map(lambda x: datetime.datetime.strptime(x.replace("/", "-"), "%Y-%m-%d").date(),
                                                           clean_valor))
                                else:
                                    clean_valor = datetime.datetime.strptime(clean_valor.replace("/", "-"), "%Y-%m-%d")
                            case "int":
                                clean_valor = list(map(int, clean_valor)) if val.get("rowwise") else int(clean_valor)
                            case "float":
                                if val.get("rowwise"):
                                    clean_valor = [float(value) if value not in ["NA", ""] else None
                                                   for value in clean_valor]
                                else:
                                    clean_valor = float(clean_valor)
                            case "coordinates (lat,long)":
                                clean_valor = [float(num) for num in clean_valor[0].split(",")]
                            case "string":
                                clean_valor = list(map(str, clean_valor)) if val.get("rowwise") else str(clean_valor)
                                clean_valor = None if all(item == '' for item in clean_valor) and clean_valor else clean_valor
                        if i == 'notes':
                            if'notes' in add_unit_inputs:
                                add_unit_inputs[i].append({f"{val.get('column')}": clean_valor})
                            else:
                                add_unit_inputs[i] = []
                                add_unit_inputs[i].append({f"{val.get('column')}": clean_valor})
                        else:
                            add_unit_inputs[i] = clean_valor
                            
                     # TODO Rethink this part   
                    if "unitcolumn" in val:
                        clean_valor2 = clean_column(
                            val.get("unitcolumn"),
                            csv_template,
                            clean=not val.get("rowwise"),
                        )
                        clean_valor2 = [
                            value if value != "NA" else None for value in clean_valor2
                        ]
                        add_unit_inputs["unitcolumn"] = clean_valor2

                    if "uncertainty" in val.keys():
                        clean_valor3 = clean_column(
                            val["uncertainty"]["uncertaintycolumn"],
                            csv_template,
                            clean=not val.get("rowwise"),
                        )
                        # clean_valor3 = [float(value) if value != 'NA' else None for value in clean_valor3]
                        add_unit_inputs["uncertainty"] = clean_valor3
                        if "uncertaintybasis" in val["uncertainty"].keys():
                            add_unit_inputs["uncertaintybasis"] = val["uncertainty"][
                                "uncertaintybasis"
                            ]
                        if "notes" in val["uncertainty"].keys():
                            add_unit_inputs["uncertaintybasis_notes"] = val[
                                "uncertainty"
                            ]["notes"]
                        else:
                            add_unit_inputs["uncertaintybasis_notes"] = None

                    samples_dict = add_unit_inputs.copy()
                    samples_dict["name"] = val.get("column")
                    samples_dict["taxonid"] = val.get("taxonid")
                    samples_dict["taxonname"] = val.get("taxonname")
                    add_units_inputs_list.append(samples_dict)

            else:
                add_unit_inputs[i] = None

        if 'notes' in add_unit_inputs.keys():
            add_unit_inputs['notes']=clean_notes(add_unit_inputs['notes'], name)
            return add_unit_inputs
        else:
            return add_unit_inputs

    elif isinstance(table, list):
        for item in table:
            results.append(pull_params(params, yml_dict, csv_template, item))
        return results