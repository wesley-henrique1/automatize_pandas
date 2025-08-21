from BASE import *
import pandas as pd


df = pd.read_excel(path.p_8596)
x = df.loc[df['RUA'].between(1,39)]

x.to_excel(power_bi.b1_prod, index= False, sheet_name= "PROD")