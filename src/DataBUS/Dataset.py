class Dataset:
    def __init__(
        self,
        datasettypeid,
        datasetid=None,
        collectionunitid=None,
        datasetname=None,
        notes=None):

        self.datasetid = datasetid
        self.collectionunitid = collectionunitid
        if isinstance(datasettypeid, int):
            self.datasettypeid = datasettypeid
        else:
            raise ValueError("Dataset type ID must be integer")
        self.datasetname = datasetname
        self.notes = notes

    def insert_to_db(self, cur):
        dataset_query = """
        SELECT ts.insertdataset(_collectionunitid:= %(collunitid)s,
                                _datasettypeid := %(datasettypeid)s,
                                _datasetname := %(datasetname)s,
                                _notes := %(notes)s);
                        """
        inputs = {
            "collunitid": self.collectionunitid,
            "datasettypeid": self.datasettypeid,
            "datasetname": self.datasetname,
            "notes": self.notes,
        }
        cur.execute(dataset_query, inputs)
        self.datasetid = cur.fetchone()[0]
        return self.datasetid

    def __str__(self):
        pass