"""Function to clean notes"""

def reorder_dict(d):
    priority = {"Original sample number": 0, "Name In Publication": 1}
    sorted_items = sorted(d.items(), key=lambda x: priority.get(x[0], float('inf')))
    return dict(sorted_items)

def clean_notes(notes = None, name = None):
    if notes:
        # List Values
        list_dict = {k: v for d in notes for k, v in d.items() if isinstance(v, list)}
        mapped_vals = [" ".join(items) for items in zip(*list_dict.values())]
        if not name:
            name = "Notes"
        mapped_vals = {name: mapped_vals}

        # Single Values
        single_vals = {k.replace('*', ''): v for d in notes for k, v in d.items() if isinstance(v, (int, float, str))}
        single_vals.update(mapped_vals)
        
        result = reorder_dict(single_vals)
        result = f"{result}"
    else:
        result = None

    return result