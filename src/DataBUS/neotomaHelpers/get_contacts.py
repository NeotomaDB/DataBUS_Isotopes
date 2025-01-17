import re

def match_abbreviation_to_full(abbreviated, full_name):
    # Split the abbreviated name into surname and initials
    parts = abbreviated.split(", ")
    surname = parts[0]
    initials = parts[1] if len(parts) > 1 else ""

    # Build a regex pattern for initials dynamically
    initial_parts = initials.split()
    regex_initials = r"\s*".join([f"{init[0]}(?:\\w*)?" for init in initial_parts])

    # Full regex pattern
    regex_pattern = fr"^{surname},\s*{regex_initials}$"

    # Perform the regex match
    return bool(re.match(regex_pattern, full_name))

def get_contacts(cur, contacts_list):
    get_contact = (
        """SELECT contactid, contactname, similarity(contactname, %(name)s) AS sim_score
                    FROM ndb.contacts
                    WHERE contactname %% %(name)s
                    ORDER BY sim_score DESC
                    LIMIT 3;"""
    )
    baseid = 1
    contids = list()
    for i in contacts_list:
        cur.execute(get_contact, {"name": i})
        data = cur.fetchone()
        if data:
            d_name = data[1]
            d_id = data[0]
            result = d_name.startswith(i.rstrip("."))
            result_2 = match_abbreviation_to_full(d_name, i)
            if (result or result_2) == True:
                contids.append({"name": d_name, "id": d_id, "order": baseid})
            else:
                contids.append({"name": i, "id": None, "order": baseid})
            baseid +=1
        else:
            contids.append({"name": i, "id": None, "order": None})

    
    return contids 
