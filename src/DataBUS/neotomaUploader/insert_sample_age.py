import DataBUS.neotomaHelpers as nh
from DataBUS import SampleAge, Response


def insert_sample_age(cur, yml_dict, csv_file, uploader):
    """
    Inserts sample age data into a database.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the inserted sample ages.
            - 'sampleAge' (list): List of IDs for the inserted sample age data.
            - 'valid' (bool): Indicates if all insertions were successful.
    """
    response = Response()
    params = ["age","chronologyid", "ageyounger", "ageolder", "uncertainty"]
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
    iterable_params['sampleid'] = uploader['samples'].sampleid
    static_params['chronologyid'] = uploader['chronology'].chronid
    for values in zip(*iterable_params.values()):
        kwargs = dict(zip(iterable_params.keys(), values))
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
            sample_age = SampleAge(**kwargs)
            response.valid.append(True)
            try:
                sample_age.insert_to_db(cur)
            except Exception as e:
                response.message.append(f"✗ Samples ages cannot be inserted: {e}")
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗ Samples ages cannot be created. {e}")
    
    response.validAll = all(response.valid)
    return response