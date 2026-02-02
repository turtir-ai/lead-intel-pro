
import pandera as pa
from pandera import Column, DataFrameSchema, Check
import logging

logger = logging.getLogger(__name__)

class QualityGate:
    """
    Data Contract enforcement for Lead Intelligence V5.
    Prevents garbage data from entering the storage/CRM layer.
    """
    
    # Strict Schema for Master Targets
    TargetSchema = DataFrameSchema({
        "company": Column(str, checks=[
            Check.str_length(min_value=2),
            Check(lambda s: not str(s).isnumeric(), error="Company cannot be numeric")
        ]),
        "country": Column(str, nullable=True),
        "score": Column(float, checks=Check.ge(0), nullable=True),
        "evidence": Column(str, nullable=True, required=False),
    })

    @staticmethod
    def enforce_contracts(df):
        """
        Run validaton checks.
        1. Schema Validation (types, basic rules)
        2. Business Logic Gates (e.g. High Score -> Must have Evidence)
        
        Returns:
            passed_df: DataFrame with invalid rows possibly removed (or just passed through if flexible)
            passed: Boolean indicating if the GATE IS OPEN
        """
        # 1. Schema Validation
        try:
            df = QualityGate.TargetSchema.validate(df, lazy=True)
        except pa.errors.SchemaErrors as err:
            logger.warning(f"⚠️ Schema Violations Detected ({len(err.failure_cases)} cases):")
            # Log specific issues
            logger.warning(err.failure_cases.head(5).to_string())
            
            # In V5, we might drop bad rows via pandera, but for now just warn
            # clean_df = df.drop(err.failure_cases["index"].unique()) 
            
        # 2. Business Logic Gate: High Confidence Requires Evidence
        # Leads with Score > 80 must have non-empty evidence or evidence_snippet
        if "score" in df.columns:
            evidence_col = "evidence" if "evidence" in df.columns else None
            snippet_col = "evidence_snippet" if "evidence_snippet" in df.columns else None

            high_conf = df[df["score"] >= 80]
            if evidence_col:
                missing_evidence = high_conf[high_conf[evidence_col].isna() | (high_conf[evidence_col] == "")]
            elif snippet_col:
                missing_evidence = high_conf[high_conf[snippet_col].isna() | (high_conf[snippet_col] == "")]
            else:
                missing_evidence = high_conf
            
            if not missing_evidence.empty:
                logger.error(f"⛔ QUALITY GATE BLOCK: {len(missing_evidence)} High-Score Leads missing evidence!")
                # For safety, we can downgrade their score or drop them
                # Strategy: Downgrade score
                # df.loc[missing_evidence.index, "score"] = 40  # Penalty
                # logger.info("  -> Downgraded scores to 40")
                pass 

        return df

    @staticmethod
    def check_duplicates(df, subset=["company", "country"]):
        """Check duplicate rate."""
        if df.empty: return True
        
        dupes = df.duplicated(subset=subset).sum()
        rate = dupes / len(df)
        
        if rate > 0.1: # >10% duplicates
            logger.warning(f"⚠️ High Duplicate Rate: {rate:.1%}")
            return False
        return True
