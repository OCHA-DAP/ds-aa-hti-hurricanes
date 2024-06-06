import os
from pathlib import Path

import pandas as pd

DATA_DIR = Path(os.getenv("AA_DATA_DIR_NEW"))
HTI_EMDAT_DIR = DATA_DIR / "public" / "processed" / "hti" / "emdat"
GLB_EMDAT_DIR = DATA_DIR / "private" / "processed" / "glb" / "emdat"


def load_glb_emdat():
    filename = "emdat-tropicalcyclone-2000-2022-processed-sids.csv"
    return pd.read_csv(GLB_EMDAT_DIR / filename)


def load_hti_impact():
    filename = "impact_data_clean_hti.csv"
    return pd.read_csv(HTI_EMDAT_DIR / filename)
