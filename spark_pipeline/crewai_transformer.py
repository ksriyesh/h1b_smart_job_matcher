from pyspark.ml import Transformer
from agents.job_scraper_agent import run_agent_query  # your CrewAI runner
from pyspark.sql import Row

class CrewAIJobFetcher(Transformer):
    def __init__(self, inputCols=["role", "company"]):
        super().__init__()
        self.inputCols = inputCols

    def _transform(self, df):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

        # Collect role and company combinations
        query_params = df.select(*self.inputCols).distinct().collect()

        all_jobs = []
        for row in query_params:
            role = row[self.inputCols[0]]
            company = row[self.inputCols[1]]
            result = run_agent_query(role, company)

            if result and result.get("results"):
                for entry in result["results"]:
                    company_name = entry["company_name"]
                    listings = entry.get("job_listings", [])
                    for job in listings:
                        all_jobs.append(Row(
                            role=role,
                            company=company_name,
                            job_title=job.get("title", ""),
                            job_url=job.get("url", ""),
                            job_text=job.get("description", "")
                        ))

        if not all_jobs:
            return spark.createDataFrame([], df.schema)

        job_df = spark.createDataFrame(all_jobs)

        # Join back to original DF (on role + company) to retain resume/context
        return df.join(job_df, on=self.inputCols, how="inner")
