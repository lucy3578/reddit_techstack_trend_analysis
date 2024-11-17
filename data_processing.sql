ALTER TABLE data_eng_keywords_clustering_updated
RENAME COLUMN c1 TO keyword;

ALTER TABLE data_eng_keywords_clustering_updated
RENAME COLUMN c2 TO result;

ALTER TABLE data_eng_keywords_clustering_updated
RENAME COLUMN c3 TO cluster;

DELETE FROM data_eng_keywords_clustering_updated
WHERE keyword = 'word';

CREATE TABLE data_eng_keywords_processed AS
SELECT keyword, cluster
FROM data_eng_keywords_clustering_updated;