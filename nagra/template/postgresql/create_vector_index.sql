CREATE INDEX "{{index_name}}" ON "{{table}}" USING HNSW
  ("{{column}}" vector_cosine_ops);
