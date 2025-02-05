from .Geog import Geog, WrongCoordinates

class CollectionUnit:
    description = "Collection Unit object in Neotoma"

    def __init__(
        self,
        collectionunitid=None,
        handle=None,
        core=None,
        siteid=None,
        colltypeid=None,
        depenvtid=None,
        collunitname=None,
        colldate=None,
        colldevice=None,
        gpsaltitude=None,
        gpserror=None,
        waterdepth=None,
        substrateid=None,
        slopeaspect=None,
        slopeangle=None,
        location=None,
        notes=None,
        geog=None):
        self.collectionunitid = collectionunitid
        if isinstance(handle, list) and len(handle) > 1:
            raise ValueError("✗ There can only be a single collection unit handle defined.")
        handle = None if handle == ["NA"] else handle
        self.handle = handle
        if self.handle is None:
            if core is not None and isinstance(core, list):
                self.handle = core[0][
                    :10
                ]  # if handle is none, use core to defind handle
        if isinstance(self.handle, list):
            self.handle = self.handle[0]
        if colltypeid is None:
            self.colltypeid = None
        else:
            self.colltypeid = int(colltypeid)

        if siteid is None:
            self.siteid = None
        else:
            self.siteid = int(siteid)
        if depenvtid is None or depenvtid == '':
            self.depenvtid = None
        else:
            self.depenvtid = int(depenvtid)

        if collunitname is None:
            self.collunitname = None
        else:
            self.collunitname = str(collunitname)

        if not colldate:
            self.colldate = None
        else:
            if isinstance(colldate, list):
                self.colldate = colldate[0]
            else:
                self.colldate = colldate

        if colldevice is None:
            self.colldevice = None
        else:
            self.colldevice = str(colldevice)

        if gpsaltitude is None:
            self.gpsaltitude = None
        else:
            self.gpsaltitude = float(gpsaltitude)

        if gpserror is None:
            self.gpserror = None
        else:
            self.gpserror = float(gpserror)

        if waterdepth is None:
            self.waterdepth = None
        else:
            self.waterdepth = float(waterdepth)

        if substrateid is None or substrateid == '':
            self.substrateid = None
        else:
            self.substrateid = int(substrateid)
        
        if slopeaspect is None:
            self.slopeaspect = None
        else:
            self.slopeaspect = int(slopeaspect)

        if slopeangle is None:
            self.slopeangle = None
        else:
            self.slopeangle = int(slopeangle)

        if not location:
            self.location = None
        else:
            if isinstance(location, list):
                self.location = str(location[0])
            else:
                self.location = location

        if notes is None:
            self.notes = None
        else:
            self.notes = str(notes)

        if not (isinstance(geog, Geog) or geog is None):
            raise TypeError("geog must be Geog or None.")
        self.geog = geog
        self.distance = None

    def __str__(self):
        statement = (
            f"Name: {self.collunitname}, "
            f"Handle: {self.handle}, "
            f"Geog: {self.geog}"
        )
        if self.distance is None:
            return statement
        else:
            return statement + f", Distance: {self.distance:<10}"

    def find_close_collunits(self, cur, distance=10000, limit=10):
        close_handles = """
                SELECT st.*, cu.handle,
                    ST_SetSRID(ST_Centroid(st.geog::geometry), 4326)::geography <-> ST_SetSRID(ST_Point(%(long)s, %(lat)s), 4326)::geography AS dist
                FROM   ndb.sites AS st
                INNER JOIN ndb.collectionunits AS cu ON cu.siteid = st.siteid
                WHERE ST_SetSRID(ST_Centroid(st.geog::geometry), 4326)::geography <-> ST_SetSRID(ST_Point(%(long)s, %(lat)s), 4326)::geography < %(distance)s
                ORDER BY dist
                LIMIT %(lim)s;"""
        cur.execute(
            close_handles,
            {
                "long": self.geog.longitude,
                "lat": self.geog.latitude,
                "distance": distance,
                "lim": limit,
            },
        )
        close_handles = cur.fetchall()
        return close_handles

    def __eq__(self, other):
        attributes = [
        'colltypeid', 'depenvtid', 'collunitname', 'colldate', 'colldevice',
        'gpsaltitude', 'gpserror', 'waterdepth', 'substrateid', 'slopeaspect',
        'slopeangle', 'location', 'notes', 'geog'
    ]
        return all(getattr(self, attr) == getattr(other, attr) for attr in attributes)
    
    def compare_cu(self, other):
        attributes = [
        'colltypeid', 'depenvtid', 'collunitname', 'colldate', 'colldevice',
        'gpsaltitude', 'gpserror', 'waterdepth', 'substrateid', 'slopeaspect',
        'slopeangle', 'location', 'notes', 'geog']

        differences = []

        for attr in attributes:
            if getattr(self, attr) != getattr(other, attr):
                differences.append(f"CSV {attr}: {getattr(self, attr)} != Neotoma {attr}: {getattr(other, attr)}")

        return differences

    def update_collunit(self, other, overwrite, cu_response=None):
        if cu_response is None:
            cu_response = type("cu_response", (), {})()  # Create a simple object
            cu_response.match = {}
            cu_response.message = []
        attributes = [
            "colltypeid",
            "depenvtid",
            "collunitname",
            "colldate",
            "colldevice",
            "gpsaltitude",
            "gpserror",
            "waterdepth",
            "substrateid",
            "slopeaspect",
            "slopeangle",
            "location",
            "notes",
            "geog",
        ]
        updated_attributes = []
        for attr in attributes:
            if getattr(self, attr) != getattr(other, attr):
                cu_response.matched[attr] = False
                cu_response.message.append(
                    f"? {attr} does not match. Update set to {overwrite[attr]}\n"
                    f"CSV File value: {getattr(self, attr)}.\n"
                    f"Neotoma value: {getattr(other, attr)}"
                )
            else:
                cu_response.valid.append(True)
                cu_response.message.append(f"✔  {attr} match.")
            if attr in overwrite and overwrite[attr]:
                setattr(self, attr, getattr(other, attr))
                updated_attributes.append(attr)
        return self

    def upsert_to_db(self, cur):
        cu_query = """SELECT upsert_collunit(_collectionunitid := %(collectionunitid)s,
                                             _handle := %(handle)s,
                                             _siteid := %(siteid)s,
                                             _colltypeid := %(colltypeid)s,
                                             _depenvtid := %(depenvtid)s,
                                             _collunitname := %(collunitname)s, 
                                             _colldate := %(colldate)s,
                                             _colldevice := %(colldevice)s,
                                             _gpslatitude := %(ns)s,
                                             _gpslongitude := %(ew)s, 
                                             _gpsaltitude := %(gpsaltitude)s,
                                             _gpserror := %(gpserror)s,
                                             _waterdepth := %(waterdepth)s,
                                             _substrateid := %(substrateid)s,
                                             _slopeaspect := %(slopeaspect)s,
                                             _slopeangle := %(slopeangle)s,
                                             _location := %(location)s,
                                             _notes := %(notes)s)
                                             """
        if not isinstance(self.geog, Geog):
            latitude = None
            longitude = None
        else:
            latitude = self.geog.latitude
            longitude = self.geog.longitude

        inputs = {
            "siteid": self.siteid,
            "collectionunitid": self.collectionunitid,
            "handle": self.handle,
            "colltypeid": self.colltypeid,
            "depenvtid": self.depenvtid,
            "collunitname": self.collunitname,
            "colldate": self.colldate,
            "colldevice": self.colldevice,
            "ns": latitude,
            "ew": longitude,
            "gpsaltitude": self.gpsaltitude,
            "gpserror": self.gpserror,
            "waterdepth": self.waterdepth,
            "substrateid": self.substrateid,
            "slopeaspect": self.slopeaspect,
            "slopeangle": self.slopeangle,
            "location": self.location,
            "notes": self.notes,
        }
        cur.execute(cu_query, inputs)
        self.collunitid = cur.fetchone()[0]
        return self.collunitid

    def insert_to_db(self, cur):
        cu_query = """SELECT ts.insertcollectionunit(
                               _handle := %(handle)s,
                               _siteid := %(siteid)s,
                               _colltypeid := %(colltypeid)s,
                               _depenvtid := %(depenvtid)s,
                               _collunitname := %(collunitname)s,
                               _colldate := %(colldate)s,
                               _colldevice := %(colldevice)s,
                               _gpslatitude := %(gpslatitude)s,  
                               _gpslongitude := %(gpslongitude)s,
                               _gpserror := %(gpserror)s,
                               _waterdepth := %(waterdepth)s,
                               _substrateid := %(substrateid)s,
                               _slopeaspect := %(slopeaspect)s,
                               _slopeangle := %(slopeangle)s,
                               _location := %(location)s,
                               _notes := %(notes)s)"""
        inputs = {
            "handle": self.handle,
            "siteid": self.siteid,
            "colltypeid": self.colltypeid,
            "depenvtid": self.depenvtid,
            "collunitname": self.collunitname,
            "colldate": self.colldate,
            "colldevice": self.colldevice,
            "gpslatitude": self.geog.latitude if self.geog is not None else None,
            "gpslongitude": self.geog.longitude if self.geog is not None else None,
            "gpserror": self.gpserror,
            "waterdepth": self.waterdepth,
            "substrateid": self.substrateid,
            "slopeaspect": self.slopeaspect,
            "slopeangle": self.slopeangle,
            "location": self.location,
            "notes": self.notes,
        }
        cur.execute(cu_query, inputs)
        self.collunitid = cur.fetchone()[0]
        return self.collunitid
