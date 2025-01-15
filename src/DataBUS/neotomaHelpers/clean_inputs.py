def clean_inputs(data):
    for key, value in data.items():
        if isinstance(value, list) and not value:
            data[key] = None
        elif value == [None] or value == ['']:
            data[key] = None
    if "siteid" in data and data["siteid"]:
        data["siteid"] = None if data["siteid"][0] in ["NA", ""] else int(data["siteid"][0])

    return data
