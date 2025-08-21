import pandas as pd
import numpy as np
from BASE import path



df_85 = pd.read_excel(path.p_8596)
df_85 = df_85.dropna(subset=['RUA'])
df_85 = df_85.loc[df_85['RUA'].between(1, 39)]
df_85.to_excel(r"C:\wh_dev\teste_df.xlsx",engine='openpyxl', index= None)
