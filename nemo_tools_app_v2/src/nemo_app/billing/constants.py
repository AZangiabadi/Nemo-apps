from __future__ import annotations

INVOICE_APPLICATION_IDENTIFIERS = (
    "Local",
    "CDG",
    "Industry",
    "External Academic",
)

ACCESS_FEE_BY_APPLICATION = {
    "Local": 50.0,
    "CDG": 50.0,
    "External Academic": 75.0,
    "Industry": 150.0,
}

PROJECT_CAP_BY_APPLICATION = {
    "Local": 1500.0,
    "CDG": 1500.0,
    "External Academic": 2500.0,
    "Industry": 4500.0,
}

DESIRED_LAB_ORDER = (
    "Cleanroom",
    "SMCL",
    "Electron Microscopy Lab",
    "Consumable",
)
DETAIL_SECTION_ORDER = (*DESIRED_LAB_ORDER, "Staff time")

TOOL_LAB_CSV = """CMP,Cleanroom
Dicing Saw,Cleanroom
TPT Wirebonder HB10,Cleanroom
AJA Orion-3 Metal Sputtering System,Cleanroom
AJA Orion-8 Dielectrics Sputtering System,Cleanroom
Angstrom High Vacuum,Cleanroom
Angstrom Metals Deposition System,Cleanroom
Cambridge NanoTech ALD,Cleanroom
Cressington Sputter Coater,Cleanroom
Edwards Thermal Evaporator 1,Cleanroom
Oxford PECVD,Cleanroom
Parylene Coater,Cleanroom
Solaris RTA,Cleanroom
Across TF1700,Cleanroom
Anatech Plasma Asher,Cleanroom
Diener Plasma Etch,Cleanroom
Oxford ICP-DRIE F-based Cobra300,Cleanroom
Oxford ICP-RIE Cl based Cobra III-V,Cleanroom
Oxford ICP RIE - direct load,Cleanroom
UVOCs UV Ozone Cleaner,Cleanroom
Beamer,Cleanroom
Elionix BODEN 50f EBL,Cleanroom
KLA P17 Profiler,Cleanroom
Lakeshore Hall System,Cleanroom
Nanomagnetics ezAFM,Cleanroom
NovaNano SEM,Cleanroom
Park AFM,Cleanroom
Woollam Alpha-SE ellipsometer,Cleanroom
Wyko NT9100 Optical Profiler,Cleanroom
BlueM Oven,Cleanroom
Heidelberg (3um) Laser Writer,Cleanroom
Heidelberg DWL 66+ Laser Writer,Cleanroom
Laurell Spinner 1,Cleanroom
Litho Hood 1 Spinner 1,Cleanroom
Litho Hood 1 Spinner 2,Cleanroom
Litho Hood 2,Cleanroom
Litho Hood 3 Spinner 3,Cleanroom
Litho - Solvent Tank,Cleanroom
Suss MA6 DUV Mask Aligner,Cleanroom
Suss MA6 Mask Aligner,Cleanroom
Vacuum Oven,Cleanroom
YES (HMDS) Oven,Cleanroom
General Acids Hood,Cleanroom
General Base Hood,Cleanroom
RCA Station,Cleanroom
ASTAR Analysis PC,Electron Microscopy Lab
EBSD Analysis PC,Electron Microscopy Lab
FEI Talos F200x S-TEM,Electron Microscopy Lab
Light Zeiss Microscope,Electron Microscopy Lab
ZEISS SEM,Electron Microscopy Lab
Diamond Saw,Electron Microscopy Lab
Dimple Grinder,Electron Microscopy Lab
FIB Sample Preparation,Electron Microscopy Lab
Grinder-Polisher,Electron Microscopy Lab
Microtome,Electron Microscopy Lab
PIPS II,Electron Microscopy Lab
Plasma Cleaner,Electron Microscopy Lab
TEM BIO Samples,Electron Microscopy Lab
Agilent 1260 Infinity GPC,SMCL
Agilent 8453 UV-Vis Spectrophotometer,SMCL
Agilent SuperNova SCXRD,SMCL
Autofinder 1,SMCL
Autofinder 2,SMCL
Bal-Tec CPD,SMCL
Bruker Dimensions FastScan AFM,SMCL
Horiba XploRA micro-Raman,SMCL
Malvern Zetasizer Nano-ZS,SMCL
Micrometrics ASAP 2020 HV BET analyzer,SMCL
PANalytical XPert3 Powder XRD,SMCL
Phi 5500 XPS,SMCL
Renishaw inVia micro-Raman,SMCL
Rigaku SmartLab XRD,SMCL
Rigaku XtaLAB Synergy-S SCXRD,SMCL
TA Instruments Q500 TGA,SMCL
Tosoh EcoSEC RI-UV GPC,SMCL
Woollam Variable Angle Ellipsometer,SMCL"""

TOOL_TO_LAB = {
    tool.strip(): lab.strip()
    for tool, lab in (line.split(",", 1) for line in TOOL_LAB_CSV.splitlines() if line.strip())
}

TOOL_MAX_HOURS_BY_TOOL_ID = {
    2: 3.0,
    3: 3.0,
    4: 3.0,
    5: 3.0,
    6: 3.0,
    7: 3.0,
    8: 8.0,
    9: 10.0,
    10: 4.0,
    11: 4.0,
    14: 4.0,
    15: 8.0,
    16: 6.0,
    18: 8.0,
    19: 8.0,
    20: 2.0,
    21: 4.0,
    22: 3.0,
    23: 9.0,
    24: 9.0,
    26: 4.0,
    27: 4.0,
    28: 4.0,
    29: 6.0,
    30: 6.0,
    31: 4.0,
    32: 6.0,
    33: 4.0,
    34: 4.0,
    35: 4.0,
    36: 4.0,
    37: 4.0,
    38: 4.0,
    39: 4.0,
    40: 3.0,
    41: 3.0,
    42: 3.0,
    43: 2.0,
    44: 4.0,
    45: 6.0,
    46: 6.0,
    47: 2.0,
    48: 3.0,
    49: 8.0,
    51: 5.0,
    52: 8.0,
    53: 9.0,
    54: 5.0,
    55: 10.0,
    56: 10.0,
    57: 8.0,
    59: 9.0,
    60: 10.0,
    61: 12.0,
    62: 10.0,
    63: 10.0,
    64: 10.0,
    65: 10.0,
    66: 12.0,
    67: 8.0,
    68: 8.0,
    69: 4.0,
    70: 4.0,
    71: 4.0,
    72: 4.0,
    73: 4.0,
    74: 4.0,
    77: 12.0,
    78: 12.0,
}

TOOL_MAX_HOURS_BY_NAME = {
    "laurellspinner": 3.0,
    "lithohood1spinner1": 3.0,
    "lithohood1spinner2": 3.0,
    "lithohood2": 3.0,
    "lithohood3spinner3": 3.0,
    "lithosolventtank": 3.0,
    "heidelberg3micronlaserwriter": 8.0,
    "heidelbergdw66laserwriter": 10.0,
    "duvma6maskaligner": 4.0,
    "ma6maskaligner": 4.0,
    "yeshmdsoven": 4.0,
    "feitalostem": 8.0,
    "zeisssigmasem": 6.0,
    "elionixboden50febl": 8.0,
    "beamer": 8.0,
    "edwardsthermalevaporator": 2.0,
    "lightzeissmicroscope": 4.0,
    "criticalpointdryer": 3.0,
    "autofinder1": 9.0,
    "autofinder2": 9.0,
    "oxfordpecvd": 4.0,
    "angstromevovacsystem": 4.0,
    "angstromhighvacuumevaporator": 4.0,
    "ajadielectricsputter": 6.0,
    "ajametalsputter": 6.0,
    "cambridgenanotechald": 4.0,
    "parylenecoater": 6.0,
    "solarisrta": 4.0,
    "acrosstf1700": 4.0,
    "oxfordicprieclbasedcobraiiiv": 4.0,
    "oxfordicpdriefbasedcobra300": 4.0,
    "oxfordicpriedirectload": 4.0,
    "dienerplasmaetch": 4.0,
    "anatechplasmaasher": 4.0,
    "rcastation": 3.0,
    "generalacidshood": 3.0,
    "generalbasehood": 3.0,
    "uvozone": 2.0,
    "tptwirebonder": 4.0,
    "dicingsaw": 6.0,
    "chemicalmechanicalpolishing": 6.0,
    "klaprofilometer": 2.0,
    "wykont9100opticalprofiler": 3.0,
    "novananosem": 8.0,
    "nanomagneticsezafm": 5.0,
    "parkafm": 8.0,
    "bet": 9.0,
    "agilentecosecgpc": 5.0,
    "agilent8453uvvisspectrophotometer": 10.0,
    "agilentsupernovascxrd": 10.0,
    "brukerdimensionfastscanafm": 8.0,
    "horibamicroraman": 9.0,
    "malvernzetasizernanozs": 10.0,
    "panalyticalxpert3powderxrd": 12.0,
    "phixps": 10.0,
    "renishawinviamicroraman": 10.0,
    "smartlabxrd": 10.0,
    "synergysscxrd": 10.0,
    "tainstrumentsq500tga": 12.0,
    "woollamvariableangleellipsometer": 8.0,
    "woollamalphaseellipsometer": 8.0,
    "pipsii": 4.0,
    "dimplegrinder": 4.0,
    "diamondsaw": 4.0,
    "plasmacleaner": 4.0,
    "grinderpolisher": 4.0,
    "microtome": 4.0,
    "tembiosamples": 12.0,
    "temfibsamplesprep": 12.0,
}

TOOL_MAX_HOURS_ALIASES = {
    "laurellspinner1": "laurellspinner",
    "heidelberg3umlaserwriter": "heidelberg3micronlaserwriter",
    "heidelbergdwl66laserwriter": "heidelbergdw66laserwriter",
    "sussma6duvmaskaligner": "duvma6maskaligner",
    "sussma6maskaligner": "ma6maskaligner",
    "feitalosf200xstem": "feitalostem",
    "zeisssem": "zeisssigmasem",
    "edwardsthermalevaporator1": "edwardsthermalevaporator",
    "angstromhighvacuum": "angstromhighvacuumevaporator",
    "angstrommetalsdepositionsystem": "angstromevovacsystem",
    "ajaorion8dielectricssputteringsystem": "ajadielectricsputter",
    "ajaorion3metalsputteringsystem": "ajametalsputter",
    "uvocsuvozonecleaner": "uvozone",
    "tptwirebonderhb10": "tptwirebonder",
    "cmp": "chemicalmechanicalpolishing",
    "klap17profiler": "klaprofilometer",
    "balteccpd": "criticalpointdryer",
    "micrometricsasap2020hvbetanalyzer": "bet",
    "tosohecosecriuvgpc": "agilentecosecgpc",
    "agilent1260infinitygpc": "agilentecosecgpc",
    "brukerdimensionsfastscanafm": "brukerdimensionfastscanafm",
    "horibaxploramicroraman": "horibamicroraman",
    "phi5500xps": "phixps",
    "rigakusmartlabxrd": "smartlabxrd",
    "rigakuxtalabsynergysscxrd": "synergysscxrd",
    "fibsamplepreparation": "temfibsamplesprep",
}

FORCED_MAX_HOURS_WITHOUT_HOURLY_CAPS = {"bet": 48.0}
