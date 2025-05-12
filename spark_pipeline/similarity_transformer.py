from pyspark.ml import Transformer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import FloatType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
import numpy as np

class CosineSimilarityScorer(Transformer):
    def __init__(self, resume_text, job_text_col="job_text", outputCol="match_score"):
        super().__init__()
        self.resume_text = resume_text
        self.job_text_col = job_text_col
        self.outputCol = outputCol

    def _transform(self, df):
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        # Step 1: Tokenize and TF-IDF the job descriptions
        tokenizer = Tokenizer(inputCol=self.job_text_col, outputCol="job_tokens")
        remover = StopWordsRemover(inputCol="job_tokens", outputCol="job_filtered")
        tf = HashingTF(inputCol="job_filtered", outputCol="job_tf", numFeatures=5000)
        idf = IDF(inputCol="job_tf", outputCol="job_tfidf")

        df = tokenizer.transform(df)
        df = remover.transform(df)
        df = tf.transform(df)
        idf_model = idf.fit(df)
        df = idf_model.transform(df)

        # Step 2: Process the resume the same way
        resume_df = spark.createDataFrame([(0, self.resume_text)], ["id", "text"])
        resume_df = tokenizer.transform(resume_df)
        resume_df = remover.transform(resume_df)
        resume_df = tf.transform(resume_df)
        resume_vec = idf_model.transform(resume_df).select("job_tfidf").first()["job_tfidf"]

        # Step 3: Cosine similarity UDF
        def cosine_similarity(vec1):
            if vec1 is None or resume_vec is None:
                return 0.0
            dot = float(vec1.dot(resume_vec))
            norm = float(np.linalg.norm(vec1.toArray()) * np.linalg.norm(resume_vec.toArray()))
            return dot / norm if norm else 0.0

        cosine_udf = udf(cosine_similarity, FloatType())
        df = df.withColumn(self.outputCol, cosine_udf(col("job_tfidf")))
        return df
