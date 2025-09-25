import pandas as pd

def build_features(matches, odds, table, weather, news):
    o = odds[odds["market"]=="1X2"].copy()
    o = o[["match_id","odd_home","odd_draw","odd_away"]].drop_duplicates("match_id")
    df = matches.merge(o, on="match_id", how="left")

    # Exemplos simples de X_*
    for c in ["odd_home","odd_draw","odd_away"]:
        df[f"X_{c}"] = df[c].astype(float)

    # Probabilidade caseira por inverso das odds (simples, substitua depois)
    inv_home, inv_draw, inv_away = 1/df["odd_home"], 1/df["odd_draw"], 1/df["odd_away"]
    s = inv_home + inv_draw + inv_away
    df["prob_home"] = (inv_home/s).clip(0,1)

    return df[["match_id","prob_home"] + [c for c in df.columns if c.startswith("X_")]]
