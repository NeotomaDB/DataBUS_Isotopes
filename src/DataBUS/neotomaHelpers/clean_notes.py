"""Function to clean notes"""

def reorder_dict(d, name = "Name In Publication"):
    priority = {"Original sample number": 0, name : 1}
    sorted_items = sorted(d.items(), key=lambda x: priority.get(x[0], float('inf')))
    return dict(sorted_items)

def clean_notes(notes = None, name = None):
    if isinstance(notes, list):
        if len(notes) == 1:
            if not list(notes[0].values())[0]:
                notes = None
    if notes:
        # List values
        list_dict = {k: v for d in notes for k, v in d.items() if isinstance(v, list)}
        mapped_vals = ["; ".join(items) for items in zip(*list_dict.values())]

        mapped_vals = list(set(mapped_vals))
        if not name:
            name = "notes"
        mapped_vals = {name: mapped_vals}
 
        # Single Values
        single_vals = {k.replace('*', ''): v for d in notes for k, v in d.items() if isinstance(v, (int, float, str))}
        single_vals.update(mapped_vals)
        
        result = reorder_dict(single_vals, name)
        result = f"{result}"
        result = result.replace("Notes:", "")

    else:
        result = None

    return result