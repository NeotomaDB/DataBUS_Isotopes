class Contact:
    def __init__(self, contactid, contactname=None, order=None):
        if isinstance(contactid, int) or contactid is None:
            self.contactid = contactid
        else:
            raise ValueError("ContactID must be an integer.")
        self.contactname = contactname
        if isinstance(order, int) or order is None:
            self.order = order
        else:
            raise ValueError("Order must be an integer or None.")

    def insert_pi(self, cur, datasetid):
        if not isinstance(datasetid, int):
            raise ValueError("DatasetID must be an integer")

        pi_query = """
        SELECT ts.insertdatasetpi(_datasetid := %(datasetid)s, 
                                  _contactid := %(contactid)s,
                                  _piorder := %(piorder)s);
                        """
        inputs = {
            "datasetid": datasetid,
            "contactid": self.contactid,
            "piorder": self.order,
        }
        cur.execute(pi_query, inputs)
        return

    def insert_data_processor(self, cur, datasetid):
        processor = """
                SELECT ts.insertdataprocessor(_datasetid := %(datasetid)s, 
                                              _contactid := %(contactid)s)
                    """
        inputs = {"datasetid": datasetid, "contactid": self.contactid}
        cur.execute(processor, inputs)
        return

    def insert_sample_analyst(self, cur, sampleid):
        sa_query = """
                   SELECT ts.insertsampleanalyst(_sampleid := %(sampleid)s,
                                                 _contactid := %(contactid)s,
                                                 _analystorder := %(analystorder)s)
                    """
        inputs = {
            "sampleid": sampleid,
            "contactid": self.contactid,
            "analystorder": self.order,
        }
        cur.execute(sa_query, inputs)
        return None
    
    def insert_collector(self, cur, collunitid):
        if not isinstance(collunitid, int):
            raise ValueError("CollectionUnitID must be an integer")

        collector_query = """
        SELECT ts.insertcollector(_collunitid := %(collunitid)s,
                           _contactid := %(contactid)s,
                           _collectororder := %(collectororder)s);
                        """
        inputs = {
            "collunitid": collunitid,
            "contactid": self.contactid,
            "collectororder": self.order,
        }
        cur.execute(collector_query, inputs)
        return

    def __str__(self):
        pass
