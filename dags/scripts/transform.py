import pandas as pd

def transform_quotes(file_path="quotes.csv"):
    df = pd.read_csv(file_path)
    
    # Example transformations
    df["text"] = df["text"].str.strip()
    df["author"] = df["author"].str.title()
    
    return df

if __name__ == "__main__":
    df = transform_quotes()
    df.to_csv("quotes_clean.csv", index=False)
