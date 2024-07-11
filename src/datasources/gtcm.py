import os
from pathlib import Path

import pandas as pd

DATA_DIR = Path(os.getenv("AA_DATA_DIR_NEW"), "")
GCTM_DIR = DATA_DIR / "collaborations" / "isi" / "GlobalTropicalCycloneModel"
GCTM_HTI_DIR = GCTM_DIR / "analysis_hti"
GCTM_HTI_IMPACT_PATH = (
    GCTM_HTI_DIR
    / "04_model_output_dataset"
    / "adm0_people_affected_predictions_hti.csv"
)


def load_gtcm_impact():
    return pd.read_csv(GCTM_HTI_IMPACT_PATH)
