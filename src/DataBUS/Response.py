class Response:
    def __init__(self, valid=None, message=None):
        self.valid = valid if valid is not None else []
        self.message = message if message is not None else []
        self.validAll = None
        self.datasetid = None
        self.datasetpi = []
        self.processor = []
        self.collector = []
        self.repoid = None
        self.databaseid = None
        self.sampleid = []
        self.data_id = []
        self.uncertaintyinputs = []

    def __str__(self):
        new_msg = "\n".join(str(m) for m in self.message)
        if self.validAll == False:
            return f"Valid: {str(self.validAll).upper()} \n" f"Message: \n" f"{new_msg}"
        else:
            return f"Valid: {self.validAll} \n" f"Message: \n" f"{new_msg}"


class SiteResponse(Response):
    def __init__(self):
        super().__init__()
        self.matched = {"namematch": False, "distmatch": False}
        self.doublematched = self.matched["namematch"] and self.matched["distmatch"]
        self.sitelist = []
        self.closesites = []
        self.siteid = None

    def __str__(self):
        response_str = super().__str__()
        # response_str += f"\n Matched: {self.doublematched}"
        if self.closesites:
            response_str += "\nClose Sites:\n" + "\n".join(
                str(site) for site in self.closesites
            )
        if self.sitelist:
            response_str += f"\n Sitelist:\n" + "\n".join(
                str(site) for site in self.sitelist
            )
        return response_str


class CUResponse(Response):
    def __init__(self):
        super().__init__()
        self.matched = {}
        # self.doublematched = (self.matched['namematch'] and self.matched['distmatch'])
        self.collunitid = None
        self.culist = []
        self.closecu = []
        self.cuid = None
        self.handle = None

    def __str__(self):
        response_str = super().__str__()
        if self.collunitid:
            response_str += f"CU ID: {self.collectionunitid}."
        if self.closecu:
            response_str += "\nClose Sites:\n" + "\n".join(
                str(site) for site in self.closecu
            )
        if self.culist:
            response_str += f"\n Sitelist:\n" + "\n".join(
                str(site) for site in self.culist
            )
        return response_str


class AUResponse(Response):
    def __init__(self):
        super().__init__()
        # self.matched = {'namematch': False, 'distmatch': False}
        # self.doublematched = (self.matched['namematch'] and self.matched['distmatch'])
        self.aulist = []
        # self.closecu = []
        self.auid = []

    def __str__(self):
        response_str = super().__str__()
        if self.aulist:
            response_str += f"\n Analysis Unit list:\n" + "\n".join(
                str(site) for site in self.aulist
            )
        return response_str


class ChronResponse(Response):
    def __init__(self):
        super().__init__()
        self.chronlist = []
        self.ccid = []
        self.chronid = None

    def __str__(self):
        response_str = super().__str__()
        if self.chronlist:
            response_str += f"\n Chronology Unit list:\n" + "\n".join(
                str(site) for site in self.chronlist
            )
        return response_str
