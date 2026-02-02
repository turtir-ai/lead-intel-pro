
import pandas as pd
import logging
try:
    from splink.duckdb.linker import DuckDBLinker
    from splink.duckdb.blocking_rule_library import block_on
    import splink.duckdb.comparison_library as cl
except ImportError:
    DuckDBLinker = None

logger = logging.getLogger(__name__)

class EntityResolver:
    """
    V5 Entity Resolution: Uses Splink for probabilistic record linkage.
    """
    
    def __init__(self):
        if not DuckDBLinker:
            logger.warning("Splink not installed. Deduplication will be skipped.")
            
    def resolve(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Run Splink deduplication on DataFrame.
        """
        if not DuckDBLinker or df.empty:
            return df

        # Ensure unique ID
        if "unique_id" not in df.columns:
            df["unique_id"] = range(0, len(df))

        # Define Settings
        settings = {
            "link_type": "dedupe_only",
            "blocking_rules_to_generate_predictions": [
                block_on("company"),
                block_on("website"),
            ],
            "comparisons": [
                cl.levenshtein_at_thresholds("company", 2),
                cl.exact_match("country", term_frequency_adjustments=True),
                cl.exact_match("website", term_frequency_adjustments=True),
            ],
        }

        try:
            linker = DuckDBLinker(df, settings)
            
            # Estimate model parameters (u and m probabilities)
            linker.estimate_u_using_random_sampling(max_pairs=10000)
            linker.estimate_m_from_label_column("company")
            
            # Predict
            df_predict = linker.predict(threshold_match_probability=0.8)
            
            # Cluster
            clusters = linker.cluster_pairwise_predictions_at_threshold(df_predict, 0.8)
            
            # Merge clusters back to main DF
            # For simplicity in this V5 implementation, we take the 'golden' record as the first in cluster
            # A more advanced logic would flatten/merge fields.
            
            # Convert clusters to pandas
            clusters_df = clusters.as_pandas_dataframe()
            
            # Map unique_id to cluster_id
            id_map = dict(zip(clusters_df["unique_id"], clusters_df["cluster_id"]))
            
            # Filter to keep only one per cluster
            # Sort by score (if exists) so we keep the best one
            if "score" in df.columns:
                df = df.sort_values("score", ascending=False)
            
            seen_clusters = set()
            indices_to_keep = []
            
            for idx, row in df.iterrows():
                uid = row["unique_id"]
                cid = id_map.get(uid, uid) # Default to self if no cluster
                
                if cid not in seen_clusters:
                    seen_clusters.add(cid)
                    indices_to_keep.append(idx)
                    
            return df.loc[indices_to_keep].drop(columns=["unique_id"])

        except Exception as e:
            logger.error(f"Splink failed: {e}")
            return df

if __name__ == "__main__":
    # Test
    data = [
        {"unique_id": 1, "company": "Mega Textile", "country": "Turkey", "website": "mega.com"},
        {"unique_id": 2, "company": "Mega Textile Ltd", "country": "Turkey", "website": "mega.com"},
        {"unique_id": 3, "company": "Other Corp", "country": "Germany", "website": "other.de"}
    ]
    df = pd.DataFrame(data)
    resolver = EntityResolver()
    resolved = resolver.resolve(df)
    print(resolved)
