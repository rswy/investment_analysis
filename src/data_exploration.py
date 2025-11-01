import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Read all CSV files from the folder
csv_files = [f for f in os.listdir('external-funds') if f.endswith('.csv')]
dataframes = {}

def combine_files():
    combined_df = pd.DataFrame()
    total_rows = 0
    for file in csv_files:
        df = pd.read_csv(f'external-funds/{file}')
        combined_df = pd.concat([combined_df, df], ignore_index=True)
        total_rows += len(df)
    
    print(f"Total rows combined: {total_rows}")
    return combined_df

def data_exploration(df):
    # Basic information
    print("\nBasic Info:")
    print(df.info())

    print(df['SYMBOL'].value_counts())
    print(df.head())
    
    # Missing values
    print("\nMissing Values:")
    missing = df.isnull().sum()
    print(missing[missing > 0])
    
    # Basic statistics
    print("\nBasic Statistics:")
    print(df.describe())
    
    # Data types check
    print("\nData Types:")
    print(df.dtypes)
    
    # Duplicate check
    duplicates = df.duplicated().sum()
    print(f"\nNumber of duplicate rows: {duplicates}")
    
    # For numeric columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        # Outlier detection using IQR
        for col in numeric_cols:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            outliers = df[(df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR))]
            print(f"\nOutliers in {col}: {len(outliers)}")




def data_exploration_price_recon(df):
    print(df.loc[df['price_difference'].abs() > 0.0001].head())
    print(df['price_difference'].abs().max())
    print(df['price_difference'].abs().min())

    
    
if __name__ == "__main__":
    # df_combined = combine_files()
    # print("Combined DataFrame Info:")
    # print(df_combined.info())
    # print(df_combined.head())
    # df_combined.to_csv('output/combined_funds_data.csv', index=False)

    
    # df = pd.read_csv("output/combined_funds_data.csv")

    # print(df['FINANCIAL TYPE'].value_counts())
    
    # print("EXPLORING DF BONDS: ===")
    # df_bonds = df.loc[df['FINANCIAL TYPE'] == 'Government Bond']
    # data_exploration(df_bonds)

    # print("\n\n\nEXPLORING DF EQUITIES: ===")
    # df_equity = df.loc[df['FINANCIAL TYPE'] == 'Equities']


    
    # data_exploration(df_equity)
    


    df_price_recon = pd.read_csv("output/price_reconciliation.csv")
    data_exploration_price_recon(df_price_recon)
