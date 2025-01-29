import warnings
import logging
import re


def retrieve_dict(yml_dict, sql_column):
    """_Get Dictionary for a Neotoma column using the YAML template_

    Args:
        yml_dict (_dict_): _The YAML template object imported by the user._
        sql_column (_str_): _A character string indicating the SQL column to be matched._

    Returns:
        list_: _A list of all dictionaries associated with a particular Neotoma table/column_
    """
    # result = next((d['column'] for d in yml_dict if d['neotoma'] == sqlColumn), None)
    # retrieving the dict instead:
    try:
        assert isinstance(yml_dict, dict)
        assert yml_dict.get("metadata")
    except AssertionError:
        logging.error(
            "The yml_dict must be a dict object (not a list) containing the key 'metadata'.",
            exc_info=True,
        )
    # result = [d for d in yml_dict['metadata'] if d['neotoma'] == sqlColumn]
    result = [d for d in yml_dict["metadata"] if re.search(fr'\b{sql_column}\b', d["neotoma"])]
    if result is None:
        warnings.warn("No matching dictionary entry found.")
    else:
        return result
