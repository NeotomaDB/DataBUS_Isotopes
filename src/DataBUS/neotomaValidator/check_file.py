import re
import os


def check_file(filename, strict = False, validation_files = "data/validation_logs/"):
    """_Validate the existence and result of a logfile._

    Args:
        filename (_str_): _The file path or relative path for a template CSV file._

    Returns:
        _dict_: _A dict type object with properties `pass` (bool), `match` (int) and `message` (str[])._
    """
    response = {"pass": False, "match": 0, "message": []}
    modified_filename = os.path.basename(filename)
    logfile = f"{validation_files}{modified_filename}"+ ".valid.log"
    not_val_logfile = f"{validation_files}not_validated/{modified_filename}"+ ".valid.log"

    if os.path.exists(logfile): 
        with open(logfile, "r", encoding="utf-8") as f:
            for line in f:
                error = re.match("✗", line)
                error2 = re.match("Valid: FALSE", line)
                if error:
                    response["match"] = response["match"] + 1
                if strict == True and error2:
                    response["match"] = response["match"] + 1
        if response["match"] == 0:
            response["pass"] = True
            response["message"].append("No errors found in the last validation.")
        else:
            response["message"].append("Errors found in the prior validation.")
    elif os.path.exists(not_val_logfile): 
        with open(not_val_logfile, "r", encoding="utf-8") as f:
            for line in f:
                error = re.match("✗", line)
                error3 = re.match(r"^\s*✗$", line)
                error2 = re.match("Valid: FALSE", line)
                if error or error3:
                    response["match"] = response["match"] + 1
                if strict == True and error2:
                    response["match"] = response["match"] + 1
        if response["match"] == 0:
            response["pass"] = True
            os.remove(not_val_logfile)
        else:
            response["message"].append("Errors found in the prior validation.")

    else:
        response["message"].append("No prior log file exists.")
        response["pass"] = True
    return response
