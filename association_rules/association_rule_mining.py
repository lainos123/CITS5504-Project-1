import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori, association_rules
import os
import shutil
from itertools import combinations

def safe_clean_output_dir(dir_path):
    """Safely clears the output directory without removing mounted volume"""
    if os.path.exists(dir_path):
        for filename in os.listdir(dir_path):
            file_path = os.path.join(dir_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')

def main():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    INPUT_PATH = os.path.join(BASE_DIR, '../data/processed/crashxfatality_df.csv')
    OUTPUT_DIR = os.path.join(BASE_DIR, 'results')
    OUTPUT_PATH = os.path.join(OUTPUT_DIR, 'association_rules.csv')
    
    # Safely clear output directory
    safe_clean_output_dir(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # [Rest of your existing code remains the same...]
    print("1. Loading and cleaning data...")
    df = pd.read_csv(INPUT_PATH)
    
    # Clean data - convert all columns to string and handle missing values
    df = df.astype(str)
    df.replace(['-9', 'nan', 'None', 'Unknown', 'Undetermined'], pd.NA, inplace=True)
    df.dropna(subset=['Road User'], inplace=True)
    
    print("2. Engineering features...")
    # Convert speed limit to categorical
    df['Speed Limit'] = pd.to_numeric(df['Speed Limit'], errors='coerce')
    df['Speed Category'] = pd.cut(df['Speed Limit'],
                                bins=[0, 60, 80, 100, 200],
                                labels=['Low', 'Medium', 'High', 'Very High'])
    
    print("3. Preparing transactions with feature=value format...")
    # Select columns to include
    cols = ['Road User', 'Age Group', 'Gender', 'Speed Category', 
            'Time of Day', 'National Road Type']
    
    # Create transactions with feature=value format
    transactions = []
    for _, row in df[cols].dropna().iterrows():
        transaction = [f"{col}={row[col]}" for col in cols]
        transactions.append(transaction)
    
    print(f"Using {len(transactions)} clean transactions")
    
    print("4. Encoding transactions...")
    te = TransactionEncoder()
    te_ary = te.fit(transactions).transform(transactions)
    encoded_df = pd.DataFrame(te_ary, columns=te.columns_)
    
    print("5. Mining association rules...")
    # Lower min_support to capture more patterns
    frequent_itemsets = apriori(encoded_df, min_support=0.05, use_colnames=True, max_len=3)
    rules = association_rules(frequent_itemsets, metric="lift", min_threshold=1.0)
    
    print(f"\nTotal rules generated: {len(rules)}")
    
    print("6. Filtering for road user rules...")
    # Find rules where Road User is in the consequent
    road_user_rules = rules[
        rules['consequents'].apply(
            lambda x: any('Road User=' in item for item in x)
        )
    ].sort_values(['lift', 'confidence'], ascending=[False, False])
    
    if len(road_user_rules) > 0:
        print(f"\nFound {len(road_user_rules)} road user rules!")
        road_user_rules.to_csv(OUTPUT_PATH, index=False)
        
        print("\nTop 5 most interesting rules:")
        top_rules = road_user_rules.head()
        for i, rule in top_rules.iterrows():
            print(f"\nRule {i+1}:")
            print(f"If {set(rule['antecedents'])}, then {set(rule['consequents'])}")
            print(f"Support: {rule['support']:.3f}, Confidence: {rule['confidence']:.3f}, Lift: {rule['lift']:.3f}")
    else:
        print("\nNo road user rules found. Debugging info:")
        print("All possible consequents:")
        print(rules['consequents'].explode().unique())
        print("\nSample frequent itemsets:")
        print(frequent_itemsets.head(10))

if __name__ == "__main__":
    main()