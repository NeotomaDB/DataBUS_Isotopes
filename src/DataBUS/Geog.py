class WrongCoordinates(Exception):
    pass


class Geog:
    def __init__(self, coords):
        if not (isinstance(coords, (list, tuple)) or coords is None):
            raise TypeError("✗ Coordinates must be a list or a tuple")
        if coords is None:
            self.latitude = None
            self.longitude = None
        else:
            if len(coords) != 2:
                raise ValueError("✗ Coordinates must have a length of 2.")
            if not (isinstance(coords[0], (int, float)) or coords[0] is None):
                raise TypeError("✗ Latitude must be a number or None.")
            if not (isinstance(coords[1], (int, float)) or coords[1] is None):
                raise TypeError("✗ Longitude must be a number or None.")
            if isinstance(coords[0], (int, float)) and not (-90 <= coords[0] <= 90):
                raise WrongCoordinates("✗ Latitude must be between -90 and 90.")
            if isinstance(coords[1], (int, float)) and not (-180 <= coords[1] <= 180):
                raise WrongCoordinates("✗ Longitude must be between -180 and 180.")
            self.latitude = coords[0]
            self.longitude = coords[1]
        if self.latitude is not None and self.longitude is not None:
            self.hemisphere = ("N" if self.latitude >= 0 else "S") + (
                "E" if self.longitude >= 0 else "W"
            )
        else:
            self.hemisphere = None

    def __eq__(self, other):
        if not isinstance(other, Geog):
            return False
        else:
            return self.latitude == other.latitude and self.longitude == other.longitude

    def __str__(self):
        return f"(Lat:{self.latitude}, Long: {self.longitude})"
