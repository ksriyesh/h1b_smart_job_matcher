from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from spark_pipeline.crewai_transformer import CrewAIJobFetcher
from spark_pipeline.similarity_transformer import CosineSimilarityScorer

def build_resume_job_match_pipeline(resume_text, selected_company, role):
    spark = SparkSession.builder.appName("SmartH1BMatcher").getOrCreate()

    # Step 1: Create base input DataFrame (single user input)
    df = spark.createDataFrame([
        (0, resume_text, selected_company, role)
    ], ["id", "resume", "company", "role"])

    # Step 2: Fetch structured job rows using CrewAI
    crew_agent = CrewAIJobFetcher(inputCols=["role", "company"])

    # Step 3: Compute similarity using job_text
    similarity = CosineSimilarityScorer(
        resume_text=resume_text,
        job_text_col="job_text",       # comes from CrewAIJobFetcher now
        outputCol="match_score"
    )

    # Step 4: Build pipeline
    pipeline = Pipeline(stages=[
        crew_agent,
        similarity
    ])

    # Step 5: Run pipeline and return DataFrame with enriched job matches
    return pipeline.fit(df).transform(df)
