-- Postgresql

CREATE TABLE sentiment_results (
    id BIGSERIAL PRIMARY KEY,
    keyword TEXT NOT NULL,
    source TEXT NOT NULL,
    post_created_time TIMESTAMP WITH TIME ZONE NOT NULL,
    post_id TEXT NOT NULL,
    post_url TEXT,
    sentiment TEXT NOT NULL,
    sentiment_score_mixed NUMERIC(4,3) NOT NULL,
    sentiment_score_positive NUMERIC(4,3) NOT NULL,
    sentiment_score_neutral NUMERIC(4,3) NOT NULL,
    sentiment_score_negative NUMERIC(4,3) NOT NULL,
    job_id TEXT NOT NULL,
    insert_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create index for querying by keyword and insert_time
CREATE INDEX idx_sentiment_results_keyword_insert_time 
ON sentiment_results(keyword, insert_time DESC);

-- Create index for just insert_time for recent records
CREATE INDEX idx_sentiment_results_insert_time 
ON sentiment_results(insert_time DESC);

ALTER TABLE sentiment_results 
ADD CONSTRAINT unique_post
UNIQUE (post_id);

