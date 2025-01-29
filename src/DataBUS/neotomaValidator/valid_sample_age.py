import DataBUS.neotomaHelpers as nh
from DataBUS import SampleAge, Response
from datetime import datetime

def valid_sample_age(cur, yml_dict, csv_file, validator):
    """
    Validates and processes sample age data for ostracode samples.
    Args:
        cur (cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (list): List of dictionaries representing CSV file data.
        validator (dict): Dictionary containing validation parameters.
    Returns:
        Response: An object containing validation results and messages.
    """
    response = Response()
    params = ["age","sampleid", "chronologyid", "ageyounger", "ageolder", "uncertainty"]
    agemodel = nh.pull_params(['agemodel'], yml_dict, csv_file, "ndb.chronologies")

    try:
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.sampleages")
    except Exception as e:
        error = str(e)
        try:
            if "time data" in error.lower():
                age_dict = nh.retrieve_dict(yml_dict, "ndb.sampleages.age")
                column = age_dict[0]['column']
                if isinstance(csv_file[0][column], str) and len(csv_file[0][column]) >= 4:
                    if len(csv_file[0][column]) == 7 and csv_file[0][column][4] == '-' and csv_file[0][column][5:7].isdigit():
                        new_date = f"{csv_file[0][column]}-01"
                        new_date = new_date.replace('-', '/')
                        new_date = datetime.strptime(new_date, "%Y/%m/%d")
                    elif csv_file[0][column][:4].isdigit():
                        new_date = int(csv_file[0][column][:4])
                    else:
                        new_date = None
                else:
                    new_date = None
            params.remove('age')
            inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.sampleages")
            inputs['age'] = new_date
            response.valid.append(True)
        except Exception as inner_e:
            response.validAll = False
            response.message.append(f"Sample Age parameters cannot be properly extracted. {e}\n {inner_e}")
            return response
    if agemodel['agemodel'].lower() == "collection date":
        if isinstance(inputs['age'], (float, int)):
            inputs['age'] = 1950 - inputs['age']
        elif isinstance(inputs['age'], datetime):
            inputs['age'] = 1950 - inputs['age'].year
        elif isinstance(inputs['age'], list):
            inputs['age'] = [1950 - value.year if isinstance(value, datetime) else 1950 - value
                             for value in inputs['age']]
    
    iterable_params = {k: v for k, v in inputs.items() if isinstance(v, list)}
    static_params = {k: v for k, v in inputs.items() if not isinstance(v, list)}
    static_params['sampleid'] = 2
    static_params['chronologyid'] = 2
    if iterable_params:
        for values in zip(*iterable_params.values()):
            kwargs = dict(zip(iterable_params.keys(), values))  # Create dictionary with lists
            kwargs.update(static_params) 
            if not(kwargs['ageyounger'] and kwargs['ageolder']):
                if kwargs['uncertainty']:
                    inputs['ageyounger'] = inputs["age"] - inputs["uncertainty"]
                    inputs['ageolder'] = inputs["age"] + inputs["uncertainty"]
                else:
                    response.message.append("? No uncertainty to substract. Ageyounger/Ageolder will be None.")
                    inputs['ageyounger'] = None
                    inputs['ageolder'] = None 
            try:
                kwargs.pop('uncertainty', None)
                SampleAge(**kwargs)
                response.valid.append(True)
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"✗ Samples ages cannot be created. {e}")
    else:
        response.message.append("? Age is a unique number. Ageyounger/Ageolder will be None.")
        try:
            static_params.pop('uncertainty', None)
            SampleAge(**static_params)
            response.valid.append(True)
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗ Samples ages cannot be created. {e}") 
        
    response.validAll = all(response.valid)
    if response.validAll:
        response.message.append(f"✔ Sample ages can be created.")
    return response