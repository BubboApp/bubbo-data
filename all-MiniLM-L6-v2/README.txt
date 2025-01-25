# Sentence Embeddings with Hugging Face and Local Models

### Files

1. **`all-MiniLM-L6-v2_from_hugg.ipynb`**:
   - This is the primary notebook that uses Hugging Face's API to fetch sentence embeddings. This method utilizes cloud-based processing, offering faster and scalable operations. It is the recommended method for embedding extraction.
   
2. **`all-MiniLM-L6-v2_local.ipynb`**:
   - This notebook serves as a backup solution and runs the embedding model locally. Although it can process sentence embeddings, it is slower and has less processing power compared to the Hugging Face API. It is suggested for use if the Hugging Face API is unavailable or for testing.

