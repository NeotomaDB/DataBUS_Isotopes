# Neotoma Isotope Schema

The isotope tables in Neotoma split into two levels of granularity, plus a set of standalone lookup tables.

## Row-Level Tables (keyed by DATA)

These tables attach to individual data rows. They describe *what* was measured, how samples were pretreated, and link measurements to specimens or stratigraphic taxa/elements.

```mermaid
---
config:
  er:
    fontSize: 14
    minEntityWidth: 180
    minEntityHeight: 40
---
erDiagram
DATA {
int dataid PK
}
SPECIMENS {
int specimenid PK
}
TAXA {
int taxonid PK
}
ELEMENTTYPES {
int elementtypeid PK
}
CONTACTS {
int contactid PK
}

ISOMETADATA {
int isometadataid PK
int dataid FK
int isomatanaltypeid FK
int isosubstratetypeid FK
int analystid FK
string lab
string labnumber
float mass_mg
float weightpercent
float atomicpercent
int reps
}
ISOSAMPLEPRETREATMENTS {
int dataid FK
int isopretreatmenttypeid FK
int order
float value
}
ISOSPECIMENDATA {
int isospecimendataid PK
int dataid FK
int specimenid FK
float sd
}
ISOSTRATDATA {
int dataid FK
int taxonid FK
int elementtypeid FK
float sd
}
ISOMATANALSUBSTRATE {
int isomatanaltypeid FK
int isosubstratetypeid FK
}

ISOMATERIALANALYZEDTYPES {
int isomatanaltypeid PK
string isomaterialanalyzedtype
}
ISOSUBSTRATETYPES {
int isosubstratetypeid PK
string isosubstratetype
}
ISOPRETREATMENTTYPES {
int isopretreatmenttypeid PK
string isopretreatmenttype
string isopretreatmentqualifier
}

DATA ||--o| ISOMETADATA : describes
DATA ||--o{ ISOSAMPLEPRETREATMENTS : "prep steps"
DATA ||--o{ ISOSPECIMENDATA : "specimen link"
DATA ||--o| ISOSTRATDATA : "taxon/element link"

SPECIMENS ||--o{ ISOSPECIMENDATA : ""
TAXA ||--o{ ISOSTRATDATA : ""
ELEMENTTYPES ||--o{ ISOSTRATDATA : ""
CONTACTS ||--o{ ISOMETADATA : "analyst"

ISOMATERIALANALYZEDTYPES ||--o{ ISOMETADATA : ""
ISOSUBSTRATETYPES ||--o{ ISOMETADATA : ""
ISOMATERIALANALYZEDTYPES ||--o{ ISOMATANALSUBSTRATE : ""
ISOSUBSTRATETYPES ||--o{ ISOMATANALSUBSTRATE : ""

ISOPRETREATMENTTYPES ||--o{ ISOSAMPLEPRETREATMENTS : ""
```

---

## Dataset-Level Tables (keyed by DATASETS + VARIABLES)

These tables describe instrumentation, standards, and scales that apply across an entire dataset/variable combination rather than to individual rows.

```mermaid
---
config:
  er:
    fontSize: 14
    minEntityWidth: 180
    minEntityHeight: 40
---
erDiagram
DATASETS {
int datasetid PK
}
VARIABLES {
int variableid PK
}

ISOINSTRUMENTATION {
int datasetid FK
int variableid FK
int isoinstrumentationtypeid FK
int isosampleintrosystemtypeid FK
float insterrorpercent
float insterrorrunsd
float insterrorlongtermpercent
}
ISOSRMETADATA {
int datasetid FK
int variableid FK
float srlocalvalue
string srlocalgeolcontext
}
ISOSTANDARDS {
int datasetid FK
int variableid FK
int isostandardid FK
float value
}
ISOVARIABLESCALETYPES {
int variableid FK
int isoscaletypeid FK
}

ISOINSTRUMENTATIONTYPES {
int isoinstrumentationtypeid PK
string isoinstrumentationtype
}
ISOSAMPLEINTROSYSTEMTYPES {
int isosampleintrosystemtypeid PK
string isosampleintrosystemtype
}
ISOSTANDARDTYPES {
int isostandardtypeid PK
string isostandardtype
string isostandardmaterial
}
ISOSCALETYPES {
int isoscaletypeid PK
string isoscaleacronym
string isoscalename
}

DATASETS ||--o{ ISOINSTRUMENTATION : ""
DATASETS ||--o{ ISOSRMETADATA : ""
DATASETS ||--o{ ISOSTANDARDS : ""

VARIABLES ||--o{ ISOINSTRUMENTATION : ""
VARIABLES ||--o{ ISOSRMETADATA : ""
VARIABLES ||--o{ ISOSTANDARDS : ""
VARIABLES ||--o{ ISOVARIABLESCALETYPES : ""

ISOINSTRUMENTATIONTYPES ||--o{ ISOINSTRUMENTATION : ""
ISOSAMPLEINTROSYSTEMTYPES ||--o{ ISOINSTRUMENTATION : ""
ISOSTANDARDTYPES ||--o{ ISOSTANDARDS : ""
ISOSCALETYPES ||--o{ ISOVARIABLESCALETYPES : ""
```

---

## Biomarker & Sample Origin Lookups

These standalone lookup tables are not currently linked to the core isotope tables above.

```mermaid
---
config:
  er:
    fontSize: 14
    minEntityWidth: 180
    minEntityHeight: 40
---
erDiagram
ISOBIOMARKERTYPES {
int isobiomarkertypeid PK
string isobiomarkertype
}
ISOBIOMARKERBANDTYPES {
int isobiomarkerbandtypeid PK
int isobiomarkertypeid FK
string isobiomarkerbandtype
}
ISOSAMPLEORIGINTYPES {
int isosampleorigintypeid PK
string isosampleorigintype
}

ISOBIOMARKERTYPES ||--o{ ISOBIOMARKERBANDTYPES : ""
```
