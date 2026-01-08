"""
MedFlow Analytics RAG Interface - FastAPI Backend
Main API server for natural language to SQL query system

Author: Zeeshan Madni
Created: 2025-12-31
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
import json
import os
from datetime import datetime
from prompts.prompt_builder import PromptBuilder
from validators.sql_validator import SQLValidator
from executors.spark_executor import SparkExecutor
from utils.schema_loader import SchemaLoader
from services.vector_store import get_vector_store
from langchain_anthropic import ChatAnthropic

# ============================================================================
# FastAPI App Configuration
# ============================================================================

app = FastAPI(
    title="MedFlow Analytics RAG API",
    description="Natural language query interface for healthcare claims data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  #TODO:Restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# Pydantic Models
# ============================================================================

class QueryRequest(BaseModel):
    """Request model for natural language query"""
    question: str = Field(..., description="Natural language question about claims data", min_length=5)
    session_id: Optional[str] = Field(None, description="Session ID for conversation tracking")

    class Config:
        json_schema_extra = {
            "example": {
                "question": "What's the average claim amount for BlueCross?",
                "session_id": "session_123"
            }
        }

class QueryResponse(BaseModel):
    """Response model for query results"""
    question: str
    sql: str
    data: List[Dict[str, Any]]
    row_count: int
    explanation: str
    execution_time_ms: float
    session_id: Optional[str] = None

    class Config:
        json_schema_extra = {
            "example": {
                "question": "What's the average claim amount for BlueCross?",
                "sql": "SELECT AVG(claim_amount) FROM healthcare.claims_clean WHERE payer = 'BlueCross'",
                "data": [{"avg_claim_amount": 2450.75}],
                "row_count": 1,
                "explanation": "The average claim amount for BlueCross is $2,450.75 based on all clean claims.",
                "execution_time_ms": 1250.5,
                "session_id": "session_123"
            }
        }

class HealthCheckResponse(BaseModel):
    """Health check response"""
    status: str
    timestamp: str
    version: str
    components: Dict[str, str]

# ============================================================================
# Initialization
# ============================================================================

# Initialize components
schema_loader = SchemaLoader()
vector_store = None  # Will be initialized on startup
prompt_builder = None  # Will be initialized after vector store
sql_validator = SQLValidator()
spark_executor = SparkExecutor()
llm = None

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    global vector_store, prompt_builder

    print("🚀 Starting MedFlow RAG API...")

    # Load schemas
    try:
        schema_loader.load_schemas()
        print("✅ Schemas loaded successfully")
    except Exception as e:
        print(f"❌ Error loading schemas: {e}")

    # Initialize vector store
    try:
        print("🔮 Initializing ChromaDB vector store...")
        vector_store = get_vector_store(persist_directory="/app/data/chromadb")

        # Check if schemas already populated
        stats = vector_store.get_stats()
        if stats['schema_count'] == 0:
            print("📥 Populating vector store with table schemas...")
            all_schemas = schema_loader.get_all_schemas()
            schemas_list = [
                {**schema_data, "table_name": table_name}
                for table_name, schema_data in all_schemas.items()
            ]
            vector_store.add_schemas_bulk(schemas_list)
            print(f"✅ Added {len(schemas_list)} schemas to vector store")
        else:
            print(f"✅ Vector store already initialized ({stats['schema_count']} schemas)")

    except Exception as e:
        print(f"⚠️  Vector store initialization failed: {e}")
        print("    Continuing without vector store (will load all schemas)")
        vector_store = None

    # Initialize prompt builder with vector store
    prompt_builder = PromptBuilder(schema_loader, vector_store=vector_store)

    # Verify Spark connection
    try:
        spark_executor.verify_connection()
        print("✅ Spark connection verified")
    except Exception as e:
        print(f"⚠️  Spark connection not available: {e}")


    # Initialize Claude LLM Client
    try:
        global llm
        api_key = os.getenv("ANTHROPIC_API_KEY")

        if not api_key:
            print("⚠️ ANTHROPIC_API_KEY not found. LLM queries will fail.")

        else:
            llm = ChatAnthropic(
                model="claude-sonnet-4-5-20250929",
                temperature=0,
                api_key=api_key
            )
            print("✅ Claude LLM client initialized")

    except Exception as e:
        print(f"⚠️ Error initializing LLM client: {e}")
        llm = None

    print("✅ API ready to accept requests")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    print("🛑 Shutting down MedFlow RAG API...")
    spark_executor.close()
    print("✅ Cleanup complete")

# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with API information"""
    return {
        "message": "MedFlow Analytics RAG API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }

@app.get("/health", response_model=HealthCheckResponse, tags=["Health"])
async def health_check():
    """
    Health check endpoint to verify API and dependent services status
    """
    components_status = {
        "api": "healthy",
        "schema_loader": "healthy" if schema_loader.is_loaded() else "not_loaded",
        "spark": "connected" if spark_executor.is_connected() else "disconnected",
    }

    overall_status = "healthy" if all(
        status in ["healthy", "connected"]
        for status in components_status.values()
    ) else "degraded"

    return HealthCheckResponse(
        status=overall_status,
        timestamp=datetime.utcnow().isoformat(),
        version="1.0.0",
        components=components_status
    )

@app.post("/query", response_model=QueryResponse, tags=["Query"])
async def execute_query(request: QueryRequest):
    """
    Execute natural language query against healthcare claims data

    **Phase 1 Implementation**: Text-to-SQL with Claude API

    Flow:
    1. Build prompt with table schemas and business rules
    2. Generate SQL using Claude API
    3. Validate SQL for safety and correctness
    4. Execute SQL via Spark
    5. Generate explanation
    6. Return results
    """
    start_time = datetime.now()

    try:
        # Step 1: Build prompt with context
        print(f"📝 Processing question: {request.question}")
        prompt = prompt_builder.build_prompt(request.question)

        # Step 2: Generate SQL (will be implemented with Claude API)
        print("🤖 Generating SQL...")
        sql = await generate_sql_with_llm(prompt)

        # Step 3: Validate SQL
        print("✅ Validating SQL...")
        validation_result = sql_validator.validate(sql)

        if not validation_result["is_valid"]:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid SQL: {validation_result['issues']}"
            )

        validated_sql = validation_result["sql"]

        # Step 4: Execute query
        print("🔄 Executing query...")
        results = spark_executor.execute(validated_sql)

        # Step 5: Generate explanation
        print("💬 Generating explanation...")
        explanation = generate_explanation(
            request.question,
            validated_sql,
            results
        )

        # Calculate execution time
        execution_time = (datetime.now() - start_time).total_seconds() * 1000

        print(f"✅ Query completed in {execution_time:.2f}ms")

        # Track query history in vector store (if available)
        if vector_store:
            try:
                vector_store.add_query_history(
                    question=request.question,
                    sql=validated_sql,
                    success=True,
                    execution_time_ms=execution_time,
                    row_count=len(results)
                )
            except Exception as ve:
                print(f"⚠️  Failed to log query history: {ve}")

        return QueryResponse(
            question=request.question,
            sql=validated_sql,
            data=results,
            row_count=len(results),
            explanation=explanation,
            execution_time_ms=execution_time,
            session_id=request.session_id
        )

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error processing query: {str(e)}")

        # Track failed query in vector store (if available)
        if vector_store:
            try:
                execution_time = (datetime.now() - start_time).total_seconds() * 1000
                vector_store.add_query_history(
                    question=request.question,
                    sql=sql if 'sql' in locals() else "",
                    success=False,
                    execution_time_ms=execution_time,
                    error_message=str(e)
                )
            except Exception as ve:
                print(f"⚠️  Failed to log query history: {ve}")

        raise HTTPException(
            status_code=500,
            detail=f"Error processing query: {str(e)}"
        )

@app.get("/schemas", tags=["Metadata"])
async def get_schemas():
    """
    Get available table schemas and metadata
    """
    try:
        schemas = schema_loader.get_all_schemas()
        return {
            "tables": list(schemas.keys()),
            "schemas": schemas
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error loading schemas: {str(e)}"
        )

@app.get("/examples", tags=["Metadata"])
async def get_example_queries():
    """
    Get categorized example queries for user guidance
    """
    try:
        # Load example queries from JSON
        with open("knowledge/examples/example_queries.json") as f:
            examples_data = json.load(f)

        # Return categorized examples for frontend
        return {
            "version": examples_data.get("version", "1.0.0"),
            "categories": examples_data.get("categories", [])
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error loading examples: {str(e)}"
        )

# ============================================================================
# Helper Functions (Placeholders for Phase 1)
# ============================================================================

async def generate_sql_with_llm(prompt: str) -> str:
    """
    Generate SQL using LLM (Claude API)
    Args: prompt: Complete prompt with schemas and question
    Returns: Generated SQL string
    Raises: If LLM client not initialized or API call fails 
    """
    
    global llm

    if llm is None:
        raise HTTPException(status_code=503, detail="LLM client not initialized. Check ANTHROPIC_API_KEY environment variable.")

    try:
        # Invoke Claude API via langchain
        response = llm.invoke(prompt)

        # Extract and clean SQL from response
        sql = response.content.strip()
        sql = sql.replace("```sql", "").replace("```", "").strip()

        return sql

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating SQL with LLM: {str(e)}")

def generate_explanation(question: str, sql: str, results: List[Dict]) -> str:
    """
    Generate natural language explanation of results

    TODO: Implement in Phase 1
    - Can use simple templates for now
    - Will enhance with LLM in later phases
    """
    # Simple template-based explanation for now
    row_count = len(results)

    if row_count == 0:
        return f"No results found for your question: '{question}'"
    elif row_count == 1:
        return f"Found 1 result for your question."
    else:
        return f"Found {row_count} results for your question."

# ============================================================================
# Error Handlers
# ============================================================================

@app.exception_handler(404)
async def not_found_handler(request, exc):
    return {
        "error": "Not Found",
        "message": "The requested endpoint does not exist",
        "docs": "/docs"
    }

@app.exception_handler(500)
async def internal_error_handler(request, exc):
    return {
        "error": "Internal Server Error",
        "message": "An unexpected error occurred. Please try again later."
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
