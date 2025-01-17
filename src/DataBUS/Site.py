from .Geog import Geog, WrongCoordinates


class Site:
    description = "Site object in Neotoma"

    def __init__(
        self,
        siteid=None,
        sitename="None",
        altitude=None,
        area=None,
        sitedescription=None,
        notes=None,
        geog=None,
    ):
        if not (isinstance(siteid, int) or siteid is None or siteid == "NA"):
            raise TypeError("✗ Site ID must be an integer or None.")
        if siteid == ["NA"] or siteid == "NA":
            siteid = None
        self.siteid = siteid

        if sitename is None:
            raise ValueError(f"✗ Sitename must be given.")
        if not isinstance(sitename, (list, str)):
            raise TypeError(f"✗ Sitename must be a string or list of strings.")
        if isinstance(sitename, str):
            sitename = [sitename]
        if isinstance(sitename, list) and len(sitename) != 1:
            raise ValueError("✗ There are multiple sitenames in your template.")
        self.sitename = sitename

        if not (isinstance(altitude, (int, float, list)) or altitude is None):
            raise TypeError("Altitude must be a number or None.")
        if isinstance(altitude, list):
            if len(altitude)>1:
                raise TypeError("Only one altitude per unit")
            if isinstance(altitude[0], (float, int)):
                self.altitude = int(altitude[0])
            else:
                self.altitude = altitude[0]
        else:
            if isinstance(altitude, (float, int)):
                self.altitude = int(altitude)
            else:
                self.altitude = altitude

        if not (isinstance(area, (int, float)) or area is None):
            raise TypeError("Area must be a number or None.")
        self.area = area

        if not (isinstance(sitedescription, (str, list)) or sitedescription is None):
            raise TypeError("Site Description must be a str or None.")
        if isinstance(sitedescription, list):
            if len(sitedescription)>1:
                raise TypeError("Only one site description per unit")
            self.sitedescription = sitedescription[0]
        else:
            self.sitedescription = sitedescription
        if isinstance(self.sitedescription, str):
            if self.sitedescription == '':
                self.sitedescription = None
        
        if isinstance(notes, list):
            notes = notes[0]
        if not (isinstance(notes, str) or notes is None):
            raise TypeError("Notes must be a str or None.")
        self.notes = notes

        if not (isinstance(geog, Geog) or geog is None):
            raise TypeError("geog must be Geog or None.")
        self.geog = geog

        self.distance = None

    def __str__(self):
        statement = (
            f"Name: {self.sitename}, " f"ID: {self.siteid}, " f"Geog: {self.geog}"
        )
        if self.distance is None:
            return statement
        else:
            return statement + f", Distance: {self.distance:<10}"

    def __eq__(self, other):
        return (
            self.siteid == other.siteid
            and self.sitename == other.sitename
            and self.altitude == other.altitude
            and self.area == other.area
            and self.sitedescription == other.sitedescription
            and self.notes == other.notes
            and self.geog == other.geog
        )

    def insert_to_db(self, cur):
        """
        NS is latitude,
        EW is longitude
        """

        site_query = """SELECT ts.insertsite(_sitename := %(sitename)s, 
                        _altitude := %(altitude)s,
                        _area := %(area)s,
                        _descript := %(sitedescription)s,
                        _notes := %(notes)s,
                        _east := %(ew)s,
                        _west := %(ew)s,
                        _north := %(ns)s,
                        _south := %(ns)s)"""
        if not self.geog:
            latitude = None
            longitude = None
        else:
            latitude = self.geog.latitude
            longitude = self.geog.longitude

        inputs = {
            "sitename": self.sitename[0],
            "altitude": self.altitude,
            "area": self.area,
            "sitedescription": self.sitedescription,
            "notes": self.notes,
            "ns": latitude,  # might be upside down
            "ew": longitude,
        }

        cur.execute(site_query, inputs)
        self.siteid = cur.fetchone()[0]
        return self.siteid

    def upsert_to_db(self, cur):
        site_query = """SELECT upsert_site(_siteid := %(siteid)s,
                                    _sitename := %(sitename)s,
                                    _altitude := %(altitude)s,
                                    _area := %(area)s,
                                    _descript := %(sitedescription)s,
                                    _notes := %(notes)s,
                                    _east := %(ew)s,
                                    _north:= %(ns)s)
                                    """
        inputs = {
            "siteid": self.siteid,
            "sitename": self.sitename[0],
            "altitude": self.altitude,
            "area": self.area,
            "sitedescription": self.sitedescription,
            "notes": self.notes,
            "ns": self.geog.latitude,
            "ew": self.geog.longitude,
        }
        cur.execute(site_query, inputs)
        self.siteid = cur.fetchone()[0]
        return self.siteid

    def find_close_sites(self, cur, dist=10000, limit=5):
        close_site = """SELECT st.*,
                ST_SetSRID(st.geog::geometry, 4326)::geography <-> ST_SetSRID(ST_Point(%(long)s, %(lat)s), 4326)::geography AS dist
            FROM   ndb.sites AS st
            WHERE ST_SetSRID(st.geog::geometry, 4326)::geography <-> ST_SetSRID(ST_Point(%(long)s, %(lat)s), 4326)::geography < %(dist)s
            ORDER BY dist
            LIMIT %(lim)s;"""
        try:
            cur.execute(
                close_site,
                {
                    "long": self.geog.longitude,
                    "lat": self.geog.latitude,
                    "dist": dist,
                    "lim": limit,
                },
            )
            close_sites = cur.fetchall()
        except Exception as e:
            close_sites = None
            print(f"Site is not formatted correctly. {e}")
        return close_sites

    def update_site(self, other, overwrite, siteresponse=None):
        if siteresponse is None:
            siteresponse = type("SiteResponse", (), {})()  # Create a simple object
            siteresponse.match = {}
            siteresponse.message = []
        attributes = [
            "sitename",
            "altitude",
            "area",
            "sitedescription",
            "notes",
            "geog",
        ]
        updated_attributes = []
        for attr in attributes:
            if getattr(self, attr) != getattr(other, attr):
                siteresponse.matched[attr] = False
                siteresponse.message.append(
                    f"? {attr} does not match. Update set to {overwrite[attr]}"
                )
            else:
                siteresponse.valid.append(True)
                siteresponse.message.append(f"✔  {attr} match.")
            if overwrite[attr]:
                setattr(self, attr, getattr(other, attr))
                updated_attributes.append(attr)
        return self

    def compare_site(self, other):
        attributes = [
        'siteid', 'sitename', 'altitude', 'area',
        'sitedescription', 'notes', 'geog']

        differences = []

        for attr in attributes:
            if getattr(self, attr) != getattr(other, attr):
                differences.append(f"CSV {attr}: {getattr(self, attr)} != Neotoma {attr}: {getattr(other, attr)}")

        return differences