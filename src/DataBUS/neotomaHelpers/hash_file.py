import hashlib
import os

def hash_file(filename, validation_files="data/validation_logs/"):
    response = {"pass": False, "hash": None, "message": []}
    modified_filename = os.path.basename(filename)
    logfile = f"{validation_files}{modified_filename}"+ ".valid.log"
    not_val_logfile = f"{validation_files}not_validated/{modified_filename}"+ ".valid.log"
    response["hash"] = hashlib.md5(open(filename, "rb").read()).hexdigest()
    response["message"].append(response["hash"])
    if os.path.exists(logfile):
        with open(logfile) as f:
            hashline = f.readline().strip("\n")
        if hashline == response["hash"]:
            response["pass"] = True
            response["message"].append("Hashes match, file hasn't changed.")
        else:
            response["message"].append(f"File has changed, validating {filename}.")
    elif os.path.exists(not_val_logfile):
        with open(not_val_logfile) as f:
            hashline = f.readline().strip("\n")
        if hashline == response["hash"]:
            response["pass"] = False
            response["message"].append("Hashes match, file hasn't been corrected.")
        else:
            response["message"].append(f"File has changed, validating {filename}.")
    else:
        response["message"].append(f"Validating {filename}.")
    return response
