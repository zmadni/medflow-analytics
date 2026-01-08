# Tech Stack Guide

## Overview

This guide provides a comprehensive breakdown of every technology used in the MedFlow Analytics RAG system, explaining what it does, why it was chosen, and how it's configured.

---

## Table of Contents

1. [Frontend: Streamlit](#frontend-streamlit)
2. [Backend: FastAPI](#backend-fastapi)
3. [LLM: Claude 3.5 Sonnet](#llm-claude-35-sonnet)
4. [Vector Store: ChromaDB](#vector-store-chromadb)
5. [Data Lake: Apache Iceberg](#data-lake-apache-iceberg)
6. [Storage: LocalStack S3](#storage-localstack-s3)
7. [Database: PostgreSQL](#database-postgresql)
8. [ETL: Apache Airflow](#etl-apache-airflow)
9. [Compute: Apache Spark](#compute-apache-spark)
10. [Containerization: Docker](#containerization-docker)
11. [Language & Runtime: Python](#language--runtime-python)

---

## Frontend: Streamlit

### What It Is
Streamlit is an open-source Python framework for creating data-focused web applications with minimal code. It turns Python scripts into interactive web apps.

### Why Streamlit?

**Pros**:
- **Rapid Development**: Build UIs with pure Python (no HTML/CSS/JavaScript)
- **Built-in Components**: Charts, tables, forms, file uploads out-of-the-box
- **Session State**: Easy state management for conversational interfaces
- **Real-time Updates**: Automatic re-rendering on user interaction
- **Data Science Friendly**: Native Pandas/Plotly integration

**Cons**:
- Limited customization compared to React/Vue
- Single-threaded (can be slow for heavy computations)
- Not ideal for complex multi-page apps

**Why Perfect for RAG Interface**:
- Chat-style interface (`st.chat_message`, `st.chat_input`)
- Quick iteration on query visualizations
- Minimal frontend expertise required

### Key Components Used

#### 1. Chat Interface
```python
# Display chat message
with st.chat_message("user"):
    st.write("Show me monthly trends")

# Chat input at bottom
prompt = st.chat_input("Ask a question about claims data...")
```

#### 2. Session State Management
```python
# Initialize persistent state
if "messages" not in st.session_state:
    st.session_state.messages = []

# Store conversation history
st.session_state.messages.append({
    "role": "assistant",
    "content": response
})
```

#### 3. Plotly Visualization
```python
# Auto-detect chart type
chart_type = detect_chart_type(df)

# Create interactive chart
if chart_type == "line":
    fig = px.line(df, x='year_month', y='total_claims', markers=True)
    st.plotly_chart(fig, use_container_width=True)
```

#### 4. Sidebar with Expanders
```python
with st.sidebar:
    st.title("🏥 MedFlow Analytics")

    # Collapsible example categories
    with st.expander("⚡ Gold Layer Analytics"):
        for example in examples:
            if st.button(example['question']):
                st.session_state.example_clicked = example['question']
                st.rerun()
```

#### 5. Data Export
```python
# CSV download button
csv = df.to_csv(index=False)
st.download_button(
    label="📥 Download Results (CSV)",
    data=csv,
    file_name="query_results.csv",
    mime="text/csv"
)
```

### Configuration

**File**: `rag_interface/frontend/pages/app.py`

**Page Config**:
```python
st.set_page_config(
    page_title="MedFlow Analytics - AI Query Interface",
    page_icon="🏥",
    layout="wide",  # Full-width layout
    initial_sidebar_state="expanded"  # Sidebar open by default
)
```

**Environment Variables**:
```bash
RAG_API_URL=http://rag-backend:8000  # Backend API endpoint
```

### Dependencies

**File**: `rag_interface/frontend/requirements.txt`
```
streamlit==1.31.0       # Core framework
pandas==2.2.0           # Data manipulation
plotly==5.18.0          # Interactive charts
requests==2.31.0        # HTTP client for API calls
```

### Running Locally
```bash
cd rag_interface/frontend
streamlit run pages/app.py --server.port 8501
```

### Best Practices in Use

1. **Component Keys**: Unique keys for buttons to avoid state conflicts
   ```python
   st.button("Click", key=f"btn_{id(message)}")
   ```

2. **Caching**: API calls cached for faster re-renders
   ```python
   @st.cache_data(ttl=300)
   def get_example_queries():
       return call_api("/examples")
   ```

3. **Error Handling**: User-friendly error messages
   ```python
   try:
       response = call_api("/query")
   except ConnectionError:
       st.error("❌ Cannot connect to API. Is the backend running?")
   ```

---

## Backend: FastAPI

### What It Is
FastAPI is a modern, high-performance Python web framework for building APIs. It's built on top of Starlette (async support) and Pydantic (data validation).

### Why FastAPI?

**Pros**:
- **Async Support**: `async/await` for non-blocking I/O (crucial for LLM API calls)
- **Automatic Documentation**: OpenAPI/Swagger UI generated automatically
- **Type Validation**: Pydantic models ensure type safety
- **High Performance**: One of the fastest Python frameworks (comparable to Node.js)
- **Developer Experience**: Auto-completion, inline errors with type hints

**Cons**:
- Smaller ecosystem than Flask/Django
- Async can be complex for beginners

**Why Perfect for RAG Backend**:
- Async LLM API calls don't block other requests
- Pydantic validation for query requests
- Auto-generated API docs for testing

### Project Structure

```
rag_interface/backend/
├── api/
│   ├── __init__.py
│   └── query_api.py         # FastAPI endpoints
├── core/
│   ├── __init__.py
│   ├── query_engine.py      # Main query orchestration
│   └── schema_loader.py     # Schema management
├── llm/
│   ├── __init__.py
│   └── claude_client.py     # Claude API wrapper
├── prompts/
│   ├── __init__.py
│   └── prompt_builder.py    # Prompt construction
├── spark/
│   ├── __init__.py
│   └── spark_manager.py     # PySpark integration
├── main.py                  # FastAPI app initialization
└── requirements.txt
```

### Key Endpoints

#### 1. Health Check
```python
@app.get("/health")
async def health_check():
    """System health status"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {
            "api": "healthy",
            "schema_loader": "healthy",
            "spark": "connected"
        }
    }
```

#### 2. Query Endpoint
```python
@app.post("/query")
async def process_query(request: QueryRequest):
    """
    Process natural language query and return SQL + results

    Request Body:
        {
            "question": "Show me monthly trends",
            "session_id": "abc123"
        }

    Response:
        {
            "question": "...",
            "sql": "SELECT ...",
            "data": [...],
            "row_count": 24,
            "execution_time_ms": 7687.4
        }
    """
    engine = QueryEngine()
    result = await engine.execute_query(request.question, request.session_id)
    return result
```

#### 3. Examples Endpoint
```python
@app.get("/examples")
async def get_examples():
    """Return categorized example queries"""
    with open("knowledge/examples/example_queries.json") as f:
        examples = json.load(f)
    return examples
```

### Pydantic Models

**Request Validation**:
```python
from pydantic import BaseModel, Field, validator

class QueryRequest(BaseModel):
    question: str = Field(..., min_length=3, max_length=500)
    session_id: Optional[str] = None

    @validator('question')
    def validate_question(cls, v):
        if not v.strip():
            raise ValueError("Question cannot be empty")
        return v.strip()

    class Config:
        schema_extra = {
            "example": {
                "question": "Show me monthly trends for all payers",
                "session_id": "test-session-001"
            }
        }
```

**Response Model**:
```python
class QueryResponse(BaseModel):
    question: str
    sql: str
    data: List[Dict]
    row_count: int
    execution_time_ms: float
    session_id: str
    explanation: Optional[str] = None
```

### Middleware & CORS

```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8501"],  # Streamlit frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

### Automatic API Documentation

**Swagger UI**: `http://localhost:8000/docs`
**ReDoc**: `http://localhost:8000/redoc`

### Dependencies

**File**: `rag_interface/backend/requirements.txt`
```
fastapi==0.109.0        # Web framework
uvicorn==0.27.0         # ASGI server
pydantic==2.5.0         # Data validation
anthropic==0.18.0       # Claude API client
pyspark==3.5.0          # Spark SQL
pyiceberg==0.5.1        # Iceberg catalog
python-dotenv==1.0.0    # Environment variables
```

### Running Locally
```bash
cd rag_interface/backend
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

---

## LLM: Claude 3.5 Sonnet

### What It Is
Claude 3.5 Sonnet is Anthropic's AI model optimized for complex reasoning, coding, and structured output tasks. It's the middle tier between Haiku (fast) and Opus (most capable).

### Why Claude 3.5 Sonnet?

**Comparison with Alternatives**:

| Feature | Claude 3.5 Sonnet | GPT-4 | GPT-3.5 |
|---------|-------------------|-------|---------|
| SQL Generation | ⭐⭐⭐⭐⭐ Excellent | ⭐⭐⭐⭐ Very Good | ⭐⭐⭐ Good |
| Context Window | 200K tokens | 128K tokens | 16K tokens |
| Speed | ~3-5 sec | ~5-10 sec | ~1-2 sec |
| Cost (per 1M tokens) | $3 input / $15 output | $10 input / $30 output | $0.50 input / $1.50 output |
| Structured Output | Excellent | Good | Fair |
| Follow Instructions | Excellent | Very Good | Good |

**Why Sonnet for This Project**:
- **SQL Expertise**: Exceptional at generating syntactically correct Spark SQL
- **Instruction Following**: Reliably returns ONLY SQL (no markdown wrappers)
- **Context Understanding**: Handles complex schema definitions
- **Table Selection**: Intelligently chooses Gold vs Silver layers
- **Cost/Performance Balance**: 3x cheaper than GPT-4, faster than Opus

### API Configuration

**Environment Variables**:
```bash
ANTHROPIC_API_KEY=sk-ant-api03-...
CLAUDE_MODEL=claude-3-5-sonnet-20241022
CLAUDE_MAX_TOKENS=2048
CLAUDE_TEMPERATURE=0  # Deterministic for SQL
```

### API Usage Pattern

```python
import anthropic

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

response = client.messages.create(
    model="claude-3-5-sonnet-20241022",
    max_tokens=2048,
    temperature=0,  # No randomness
    messages=[
        {
            "role": "user",
            "content": prompt  # System prompt + user question
        }
    ]
)

sql = response.content[0].text
```

### Temperature Settings

| Temperature | Use Case | Output |
|-------------|----------|--------|
| 0 (used) | SQL generation | Deterministic, consistent queries |
| 0.3-0.5 | Creative text | Some variation, still focused |
| 0.7-1.0 | Brainstorming | High creativity, less consistent |

**Why Temperature=0**:
- Same question → Same SQL (predictable)
- No hallucinated column names
- Consistent table selection logic

### Token Management

**Typical Request**:
```
System Prompt (1,800 tokens)
├── Table schemas: ~1,200 tokens
├── Business rules: ~200 tokens
├── Examples: ~300 tokens
└── Instructions: ~100 tokens

User Question: ~20 tokens

Total Input: ~1,820 tokens
```

**Typical Response**:
```
SQL Query: ~100-300 tokens
```

**Cost Per Query**: ~$0.005-0.01 (half a penny)

### Rate Limits

**Anthropic API Limits** (Tier 2):
- 50 requests per minute
- 100,000 tokens per minute
- ~30 concurrent queries supported

**Handling Rate Limits**:
```python
try:
    response = client.messages.create(...)
except anthropic.RateLimitError:
    # Wait 60 seconds and retry
    time.sleep(60)
    response = client.messages.create(...)
```

### Claude vs Other LLMs for SQL

**Claude Advantages**:
1. **No Markdown Formatting**: Returns clean SQL (GPT often adds ```sql```)
2. **Complex JOIN Logic**: Better at multi-table queries
3. **Date Handling**: Understands `year_month` string format
4. **Partition Awareness**: Uses partition columns when beneficial

**Example GPT-4 Issue**:
```sql
-- GPT-4 often returns this (with markdown):
```sql
SELECT * FROM claims WHERE year_month = '2024-01'
```

-- Claude returns clean SQL:
SELECT * FROM claims WHERE year_month = '2024-01'
```

---

## Vector Store: ChromaDB

**Added**: 2026-01-08

### What It Is
ChromaDB is an open-source embedding database designed for AI applications. It provides vector similarity search for semantic retrieval of documents, with built-in embedding generation and persistence.

### Why ChromaDB?

**Pros**:
- **Python-Native**: Simple API, no separate server required
- **Auto-Embeddings**: Built-in sentence transformers for text embedding
- **Fast Similarity Search**: Efficient cosine similarity with HNSW indexing
- **Persistent Storage**: File-based storage that survives restarts
- **Metadata Filtering**: Combine vector search with WHERE-like conditions
- **Lightweight**: Minimal dependencies, runs in-process

**Cons**:
- Single-machine only (not distributed like Pinecone/Weaviate)
- Limited to ~1M vectors before performance degrades
- No built-in security/authentication

**Alternatives Considered**:
- **Pinecone**: Cloud-only, expensive ($70+/month), overkill for our scale
- **Weaviate**: Requires separate server, more complex setup
- **FAISS**: Lower-level, no persistence, more manual work
- **Qdrant**: Similar to ChromaDB but less mature Python SDK

**Why Perfect for This Project**:
- Embedded in FastAPI (no extra service)
- Free and open-source
- Perfect for our scale (< 100K vectors)
- Persistent across container restarts
- Learning as a bonus (modern vector DB experience)

### Key Features Used

#### 1. Collections

We use two collections for different purposes:

```python
# Schema collection: table definitions for semantic search
schema_collection = client.get_or_create_collection(
    name="table_schemas",
    embedding_function=embedding_function,
    metadata={"description": "Table schemas for semantic search"}
)

# History collection: successful queries for learning
history_collection = client.get_or_create_collection(
    name="query_history",
    embedding_function=embedding_function,
    metadata={"description": "Query history for few-shot learning"}
)
```

#### 2. Embedding Function

```python
from chromadb.utils import embedding_functions

# SentenceTransformer embedding model
embedding_function = embedding_functions.SentenceTransformerEmbeddingFunction(
    model_name="all-MiniLM-L6-v2"  # 384 dimensions, fast, good quality
)
```

**Model Choice**:
- **all-MiniLM-L6-v2**: Lightweight (22MB), fast inference (~5ms), good quality
- **Alternatives**: all-mpnet-base-v2 (better quality but 2x slower), BGE embeddings

#### 3. Semantic Search

```python
# Find top-3 relevant schemas for user question
results = schema_collection.query(
    query_texts=["Show me monthly trends for all payers"],
    n_results=3,
    where=None  # Optional metadata filters
)

# Returns:
# - ids: ["iceberg.gold.claims_monthly_summary", ...]
# - distances: [0.435, 0.538, 0.540]  # Lower = more similar
# - metadatas: [{"layer": "gold", "column_count": 15}, ...]
```

#### 4. Document Storage

```python
# Add schema to collection
schema_collection.add(
    ids=[table_name],
    documents=[formatted_schema_text],
    metadatas=[{
        "table_name": table_name,
        "description": description,
        "column_count": len(columns),
        "layer": "gold",
        "added_at": datetime.now().isoformat()
    }]
)
```

### Configuration

#### Persistence

```python
import chromadb
from chromadb.config import Settings

# Persistent client with local storage
client = chromadb.PersistentClient(
    path="/app/data/chromadb",
    settings=Settings(
        anonymized_telemetry=False,  # Opt-out of telemetry
        allow_reset=True,            # Allow collection reset (dev only)
    )
)
```

#### Docker Volume

```yaml
# docker-compose.yml
services:
  rag-backend:
    volumes:
      - chromadb-data:/app/data/chromadb  # Persistent vector storage

volumes:
  chromadb-data:  # Named volume survives container restarts
```

### Use Cases in Our System

#### Use Case 1: Semantic Schema Retrieval

**Problem**: Loading all 5 schemas into every prompt wastes tokens (1,487 tokens → expensive at scale)

**Solution**: Vector search finds only top-3 relevant schemas

```python
# User asks: "Show me monthly trends"
relevant_schemas = vector_store.get_relevant_schemas(
    query="Show me monthly trends",
    n_results=3,
    min_similarity=0.3
)

# Returns:
# 1. iceberg.gold.claims_monthly_summary (similarity: 0.565) ← Perfect!
# 2. iceberg.gold.provider_performance_metrics (similarity: 0.462)
# 3. iceberg.gold.claims_approval_funnel (similarity: 0.460)
```

**Result**: 23% token reduction (342 tokens saved per query)

#### Use Case 2: Query History Learning

**Problem**: Static few-shot examples don't adapt to user patterns

**Solution**: Retrieve similar past successful queries as dynamic examples

```python
# Log successful query
vector_store.add_query_history(
    question="Show me monthly trends for all payers",
    sql="SELECT year_month, payer_name, total_claims...",
    success=True,
    execution_time_ms=7563.73,
    row_count=24
)

# Later, retrieve similar queries for a new question
similar = vector_store.get_similar_queries(
    query="What are the monthly trends?",
    n_results=3,
    success_only=True
)

# Uses similar queries as few-shot examples in prompt
```

**Result**: Better SQL quality, system learns from success patterns

### Performance Metrics

**Real-World Results** (Current System):

| Metric | Value |
|--------|-------|
| Schemas stored | 5 tables |
| Queries tracked | 6+ (growing) |
| Embedding dimensions | 384 (all-MiniLM-L6-v2) |
| Query time | ~5ms (in-memory) |
| Token reduction | 23% (342 tokens/query) |
| Cost savings | $0.001/query |

**Scaling Projections**:

| Tables | Token Savings | Monthly Cost Savings (10K queries) |
|--------|---------------|------------------------------------|
| 5 | 23% | $10.26 |
| 20 | ~60% | $40+ |
| 50 | ~75% | $100+ |
| 100+ | ~80%+ | $200+ |

### Best Practices

1. **Persistent Storage**: Always use `PersistentClient` with volume mounts
2. **Collection Design**: Separate collections for different data types (schemas vs history)
3. **Metadata**: Store rich metadata for filtering and debugging
4. **Similarity Threshold**: Set min_similarity to filter out irrelevant results
5. **Batch Operations**: Use `add_batch()` for initial population
6. **Regular Cleanup**: Periodically prune old/failed queries from history

### Learning Resources

- **ChromaDB Docs**: [docs.trychroma.com](https://docs.trychroma.com/)
- **Sentence Transformers**: [sbert.net](https://www.sbert.net/)
- **Vector Search**: [Pinecone Vector Database Guide](https://www.pinecone.io/learn/vector-database/)
- **Embeddings**: [OpenAI Embeddings Guide](https://platform.openai.com/docs/guides/embeddings)

---

## Data Lake: Apache Iceberg

### What It Is
Apache Iceberg is an open table format for huge analytic datasets. It provides ACID transactions, schema evolution, and time travel for data lakes.

### Why Iceberg?

**Traditional Data Lake Problems**:
- ❌ No ACID transactions (partial writes, inconsistent reads)
- ❌ Schema changes break queries
- ❌ Slow queries (full table scans)
- ❌ No rollback capabilities

**Iceberg Solutions**:
- ✅ ACID transactions (atomic commits)
- ✅ Schema evolution (add/drop columns safely)
- ✅ Hidden partitioning (automatic partition pruning)
- ✅ Time travel (query historical snapshots)
- ✅ Partition evolution (change partitioning without rewriting data)

### Comparison with Alternatives

| Feature | Iceberg | Delta Lake | Hudi |
|---------|---------|------------|------|
| ACID Transactions | ✅ Yes | ✅ Yes | ✅ Yes |
| Time Travel | ✅ Yes | ✅ Yes | ✅ Yes |
| Schema Evolution | ⭐⭐⭐⭐⭐ Best | ⭐⭐⭐⭐ Good | ⭐⭐⭐ Good |
| Multi-Engine Support | ⭐⭐⭐⭐⭐ Best | ⭐⭐⭐ Good | ⭐⭐⭐ Good |
| Hidden Partitioning | ✅ Yes | ❌ No | ❌ No |
| Open Standard | ✅ Apache | ❌ Databricks | ✅ Apache |

**Why Iceberg for This Project**:
- **Multi-Engine**: Works with Spark, Trino, Flink, DuckDB
- **Hidden Partitioning**: Users don't write partition filters (automatic)
- **Schema Evolution**: Can add `approval_rate_v2` without breaking existing queries
- **Performance**: Metadata-based operations (fast)

### Table Structure

**MedFlow Iceberg Catalog**:
```
iceberg (catalog)
├── silver (schema)
│   ├── claims (partitioned by: payer_name, year_month)
│   ├── claims_quarantine (partitioned by: payer_name)
│   └── processing_log
└── gold (schema)
    ├── claims_monthly_summary (partitioned by: year_month, payer_name)
    ├── claims_approval_funnel (partitioned by: year_month, payer_name)
    └── provider_performance_metrics (unpartitioned)
```

### Partitioning Strategy

**Silver Layer** (`claims`):
```python
# Hidden partitioning definition
partition_spec = [
    PartitionField(source_id=col_id("payer_name"), field_id=1000,
                   transform=IdentityTransform(), name="payer_name"),
    PartitionField(source_id=col_id("year_month"), field_id=1001,
                   transform=IdentityTransform(), name="year_month")
]
```

**Query Example**:
```sql
-- User writes simple query
SELECT * FROM iceberg.silver.claims
WHERE payer_name = 'BlueCross'
  AND year_month = '2025-12'

-- Iceberg automatically reads only relevant partition files:
-- s3://medflow-datalake/silver/claims/payer_name=BlueCross/year_month=2025-12/*.parquet
```

**Performance**: Reads ~1% of data (99% pruned)

### Schema Evolution Example

**Add Column Without Breaking Queries**:
```sql
-- Add new column (backward compatible)
ALTER TABLE iceberg.silver.claims
ADD COLUMN claim_priority STRING

-- Old queries still work (new column is NULL)
SELECT claim_id, patient_id FROM iceberg.silver.claims

-- New queries can use new column
SELECT claim_id, claim_priority FROM iceberg.silver.claims
```

### Time Travel

**Query Historical Data**:
```sql
-- Query data as of specific timestamp
SELECT * FROM iceberg.silver.claims
FOR SYSTEM_TIME AS OF '2025-12-01 00:00:00'

-- Query specific snapshot
SELECT * FROM iceberg.silver.claims
FOR SYSTEM_VERSION AS OF 1234567890
```

**Use Cases**:
- Audit data changes
- Recover from bad writes
- Compare current vs historical metrics

### File Format

**Parquet + Iceberg Metadata**:
```
s3://medflow-datalake/
├── silver/
│   └── claims/
│       ├── metadata/
│       │   ├── v1.metadata.json       # Table metadata
│       │   ├── snap-123.avro          # Snapshot manifest
│       │   └── manifest-abc.avro      # File manifest
│       └── data/
│           ├── payer_name=BlueCross/
│           │   ├── year_month=2025-12/
│           │   │   └── data-001.parquet  # Actual data
│           │   └── year_month=2026-01/
│           │       └── data-002.parquet
│           └── payer_name=Aetna/
│               └── ...
```

### Configuration in Spark

```python
spark = SparkSession.builder \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://medflow-datalake/") \
    .getOrCreate()
```

---

## Storage: LocalStack S3

### What It Is
LocalStack is a fully functional local AWS cloud stack. It emulates AWS services (S3, DynamoDB, Lambda, etc.) for local development and testing.

### Why LocalStack?

**Problems with AWS S3**:
- ❌ Costs money for development
- ❌ Requires internet connection
- ❌ Slower iteration (network latency)
- ❌ Risk of accidental production data exposure

**LocalStack Benefits**:
- ✅ Free and open source
- ✅ Works offline
- ✅ Fast (local file system)
- ✅ No AWS account needed
- ✅ Same API as real S3 (drop-in replacement)

### Configuration

**Docker Compose**:
```yaml
localstack:
  image: localstack/localstack:3.0
  ports:
    - "4566:4566"  # LocalStack gateway
  environment:
    - SERVICES=s3              # Only S3 service
    - DEBUG=1
    - DATA_DIR=/tmp/localstack # Persistence
  volumes:
    - "./data/localstack:/tmp/localstack"
```

**S3 Endpoint Configuration** (Spark):
```python
.config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566")
.config("spark.hadoop.fs.s3a.access.key", "test")
.config("spark.hadoop.fs.s3a.secret.key", "test")
.config("spark.hadoop.fs.s3a.path.style.access", "true")  # Critical for LocalStack
```

### Bucket Structure

**MedFlow S3 Bucket** (`medflow-datalake`):
```
s3://medflow-datalake/
├── bronze/
│   └── claims/
│       └── raw_*.json
├── silver/
│   └── claims/
│       └── (Iceberg tables)
└── gold/
    ├── claims_monthly_summary/
    ├── claims_approval_funnel/
    └── provider_performance_metrics/
```

### Creating Bucket

**Using AWS CLI** (pointing to LocalStack):
```bash
aws --endpoint-url=http://localhost:4566 s3 mb s3://medflow-datalake
```

**Using Python**:
```python
import boto3

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test'
)

s3.create_bucket(Bucket='medflow-datalake')
```

### Path Style Access

**Critical Configuration**:
```python
"spark.hadoop.fs.s3a.path.style.access": "true"
```

**Why Needed**:
- **Virtual Host Style**: `https://bucket-name.s3.amazonaws.com/key`
- **Path Style**: `https://s3.amazonaws.com/bucket-name/key`
- LocalStack requires **path style**

### Persistence

**Data Persists Between Container Restarts**:
```yaml
volumes:
  - "./data/localstack:/tmp/localstack"
```

**Location**: `/home/zmadni/Projects/medflow-analytics/data/localstack/`

### Transitioning to Real AWS S3

**Change 3 lines**:
```python
# From:
.config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566")
.config("spark.hadoop.fs.s3a.access.key", "test")
.config("spark.hadoop.fs.s3a.secret.key", "test")

# To:
# (Remove endpoint config, use real credentials)
.config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY"))
.config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY"))
```

---

## Database: PostgreSQL

### What It Is
PostgreSQL is a powerful, open-source object-relational database system. In this project, it serves as the metadata backend for Apache Airflow.

### Why PostgreSQL?

**Airflow Metadata Requirements**:
- Store DAG definitions and run history
- Task state management
- Connection credentials
- Variable storage

**PostgreSQL Advantages**:
- ✅ ACID compliance
- ✅ Rich data types (JSON, Arrays)
- ✅ Strong concurrency control
- ✅ Widely supported by Airflow

### Configuration

**Docker Compose**:
```yaml
postgres:
  image: postgres:15
  environment:
    POSTGRES_USER: airflow
    POSTGRES_PASSWORD: airflow
    POSTGRES_DB: airflow
  ports:
    - "5432:5432"
  volumes:
    - postgres-db:/var/lib/postgresql/data
```

**Airflow Connection String**:
```
postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
```

### Schema

**Airflow Creates Tables Automatically**:
```sql
-- DAG metadata
dag
dag_run
dag_tag

-- Task execution
task_instance
task_fail
task_reschedule

-- Connections & variables
connection
variable

-- Logging
log
import_error
```

### Not Used For

**PostgreSQL is NOT used for**:
- ❌ Claims data storage (that's in Iceberg/S3)
- ❌ RAG query results (ephemeral, returned via API)
- ❌ LLM prompt caching

**Only Used For**: Airflow orchestration metadata

---

## ETL: Apache Airflow

### What It Is
Apache Airflow is a platform to programmatically author, schedule, and monitor workflows. It uses Directed Acyclic Graphs (DAGs) to define task dependencies.

### Why Airflow?

**ETL Requirements**:
- Schedule Bronze → Silver → Gold transformations
- Retry failed tasks
- Monitor pipeline health
- Manage dependencies between tasks

**Airflow Solutions**:
- ✅ Python-based DAG definitions (code as config)
- ✅ Rich UI for monitoring
- ✅ Retry logic and error handling
- ✅ Scalable (can add workers)
- ✅ Extensive integrations (Spark, S3, etc.)

### Configuration

**Docker Compose**:
```yaml
airflow-webserver:
  image: apache/airflow:2.8.0
  command: webserver
  ports:
    - "8080:8080"
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
```

**Access**: `http://localhost:8080`
- **Username**: `admin`
- **Password**: `admin`

### DAG Structure

**Example DAG** (Bronze → Silver):
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'medflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'bronze_to_silver_claims',
    default_args=default_args,
    description='Transform raw claims to Silver layer',
    schedule_interval='@daily',  # Run daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    extract_bronze = PythonOperator(
        task_id='extract_bronze_claims',
        python_callable=extract_from_bronze,
    )

    validate_data = PythonOperator(
        task_id='validate_claims_data',
        python_callable=validate_claims,
    )

    load_silver = PythonOperator(
        task_id='load_to_silver_iceberg',
        python_callable=write_to_silver,
    )

    # Define dependencies
    extract_bronze >> validate_data >> load_silver
```

**Dependency Graph**:
```
extract_bronze → validate_data → load_silver
```

### Medallion Pipeline DAGs

**Planned DAGs**:
1. `bronze_ingestion`: Raw files → Bronze (S3)
2. `silver_transformation`: Bronze → Silver (quality checks)
3. `gold_aggregation`: Silver → Gold (pre-aggregation)
4. `rag_schema_sync`: Update RAG schema JSONs

### Monitoring

**Airflow UI Tabs**:
- **DAGs**: View all workflows
- **Grid**: Task execution grid
- **Graph**: Visual dependency graph
- **Logs**: Task execution logs
- **Admin > Connections**: Configure external systems

---

## Compute: Apache Spark

### What It Is
Apache Spark is a unified analytics engine for large-scale data processing. It provides APIs in Python (PySpark), Scala, and Java for distributed computing.

### Why Spark?

**Requirements**:
- Query Iceberg tables from Python
- Handle 100K+ claim records efficiently
- Support complex SQL (window functions, CTEs)
- Parallel processing for aggregations

**Spark Solutions**:
- ✅ Native Iceberg integration
- ✅ Distributed SQL engine
- ✅ In-memory caching for speed
- ✅ Supports Spark SQL (SQL interface)

### Configuration

**PySpark Session Setup**:
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MedFlow-RAG-Query-Engine") \
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
            "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://medflow-datalake/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
```

### Executing SQL Queries

```python
# Execute SQL against Iceberg tables
df = spark.sql("""
    SELECT year_month, payer_name, total_claims
    FROM iceberg.gold.claims_monthly_summary
    WHERE year_month >= '2025-01'
    ORDER BY year_month, payer_name
""")

# Convert to Pandas for JSON serialization
pandas_df = df.toPandas()
results = pandas_df.to_dict(orient='records')
```

### Performance Tuning

**Executor Configuration**:
```python
.config("spark.executor.memory", "2g")
.config("spark.driver.memory", "2g")
.config("spark.sql.shuffle.partitions", "4")  # Small dataset
```

**Caching Frequently Accessed Tables**:
```python
spark.sql("CACHE TABLE iceberg.gold.claims_monthly_summary")
```

---

## Containerization: Docker

### What It Is
Docker packages applications and dependencies into containers that run consistently across environments.

### Why Docker?

**Problems Without Docker**:
- ❌ "Works on my machine" syndrome
- ❌ Complex dependency installation
- ❌ Conflicting Python versions
- ❌ Manual service orchestration

**Docker Solutions**:
- ✅ Consistent environment (dev = prod)
- ✅ Isolated dependencies per service
- ✅ Easy scaling (add more containers)
- ✅ docker-compose for multi-container orchestration

### Docker Compose Architecture

**Services**:
```yaml
services:
  rag-backend:        # FastAPI + PySpark
  rag-frontend:       # Streamlit UI
  localstack:         # S3 emulator
  postgres:           # Airflow metadata
  spark-master:       # Spark standalone cluster
  # (Future: airflow-webserver, airflow-scheduler)
```

**Network**:
```yaml
networks:
  medflow-network:
    driver: bridge
```

All services communicate via `medflow-network` using service names as hostnames.

### Key Dockerfiles

**Backend** (`rag_interface/docker/backend.Dockerfile`):
```dockerfile
FROM python:3.11-bookworm

# Install Java (required for PySpark)
RUN apt-get update && apt-get install -y openjdk-17-jdk-headless

# Install uv (fast Python package manager)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

WORKDIR /app

# Install dependencies
COPY backend/requirements.txt .
RUN uv pip install --system -r requirements.txt

# Copy application code
COPY backend/ .
COPY schemas/ /app/schemas/
COPY knowledge/ /app/knowledge/

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

**Frontend** (`rag_interface/docker/frontend.Dockerfile`):
```dockerfile
FROM python:3.11-slim

RUN apt-get update && apt-get install -y curl
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

WORKDIR /app

COPY frontend/requirements.txt .
RUN uv pip install --system -r requirements.txt

COPY frontend/ .

CMD ["streamlit", "run", "pages/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

### Volumes

**Persistent Data**:
```yaml
volumes:
  postgres-db:                     # Airflow metadata
  localstack-data:                 # S3 bucket data
    driver: local
    driver_opts:
      type: none
      o: bind
      device: ./data/localstack
```

**Bind Mounts** (for development):
```yaml
volumes:
  - ./rag_interface/backend:/app  # Live code reload
```

---

## Language & Runtime: Python

### Version
**Python 3.11**

### Why Python 3.11?

**Improvements Over 3.10**:
- 10-60% faster (optimized CPython bytecode)
- Better error messages
- `asyncio` performance improvements
- Exception groups (better error handling)

### Key Libraries

**Data Processing**:
- `pandas==2.2.0`: Data manipulation
- `numpy==1.26.0`: Numerical computing
- `pyspark==3.5.0`: Distributed data processing

**Web Framework**:
- `fastapi==0.109.0`: REST API
- `uvicorn==0.27.0`: ASGI server
- `streamlit==1.31.0`: Frontend

**LLM Integration**:
- `anthropic==0.18.0`: Claude API client

**Data Lake**:
- `pyiceberg==0.5.1`: Iceberg Python SDK

**Storage**:
- `boto3==1.34.0`: AWS SDK (for LocalStack S3)

### Package Manager: uv

**Why uv instead of pip**:
- ⚡ 10-100x faster than pip
- 🔒 Reliable dependency resolution
- 📦 Cargo-like workflow (Rust tooling)

**Installation**:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Usage**:
```bash
uv pip install -r requirements.txt
```

---

## Technology Decision Matrix

| Requirement | Technology | Alternative Considered | Why Chosen |
|-------------|-----------|------------------------|------------|
| Frontend UI | Streamlit | React + Plotly | Faster development, Python-native |
| Backend API | FastAPI | Flask | Async support, auto docs |
| LLM | Claude 3.5 Sonnet | GPT-4 | Better SQL, cheaper, cleaner output |
| Data Lake Format | Iceberg | Delta Lake | Better schema evolution, multi-engine |
| Storage | LocalStack S3 | MinIO | Drop-in AWS S3 replacement |
| Database | PostgreSQL | MySQL | Better JSON support, Airflow default |
| Orchestration | Airflow | Dagster, Prefect | Industry standard, mature ecosystem |
| Compute | Spark | DuckDB | Iceberg integration, distributed |
| Containerization | Docker Compose | Kubernetes | Simpler for local dev, easier learning |

---

## Next Steps

- See [DOCKER_ARCHITECTURE.md](./DOCKER_ARCHITECTURE.md) for detailed container breakdown
- Review [CODE_WALKTHROUGH.md](./CODE_WALKTHROUGH.md) for implementation details
- Check [INTEGRATION_GUIDE.md](./INTEGRATION_GUIDE.md) for inter-service communication

---

**Last Updated**: 2026-01-07
