"""
MedFlow Analytics RAG Interface - Streamlit Frontend
Natural language query interface for healthcare claims data

Author: Zeeshan Madni
Created: 2025-12-31
"""

import streamlit as st
import requests
import pandas as pd
import json
import os
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Any, Optional, Tuple

# ============================================================================
# Configuration
# ============================================================================

API_BASE_URL = os.getenv("RAG_API_URL", "http://localhost:8000")

st.set_page_config(
    page_title="MedFlow Analytics - AI Query Interface",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# Session State Initialization
# ============================================================================

if "messages" not in st.session_state:
    st.session_state.messages = []

if "session_id" not in st.session_state:
    import uuid
    st.session_state.session_id = str(uuid.uuid4())

# ============================================================================
# Helper Functions
# ============================================================================

def call_api(endpoint: str, method: str = "GET", data: Dict = None) -> Dict:
    """
    Call RAG API endpoint

    Args:
        endpoint: API endpoint path
        method: HTTP method (GET, POST)
        data: Request data for POST

    Returns:
        API response as dictionary
    """
    url = f"{API_BASE_URL}{endpoint}"

    try:
        if method == "GET":
            response = requests.get(url, timeout=30)
        else:
            response = requests.post(url, json=data, timeout=30)

        response.raise_for_status()
        return response.json()

    except requests.exceptions.ConnectionError:
        st.error(f"❌ Cannot connect to API at {API_BASE_URL}. Is the backend running?")
        st.stop()
    except requests.exceptions.Timeout:
        st.error("❌ Request timeout. Query took too long to execute.")
        st.stop()
    except requests.exceptions.HTTPError as e:
        st.error(f"❌ API Error: {e.response.text}")
        st.stop()
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        st.stop()

def execute_query(question: str) -> Dict:
    """
    Execute natural language query via API

    Args:
        question: User's question

    Returns:
        Query response dictionary
    """
    payload = {
        "question": question,
        "session_id": st.session_state.session_id
    }

    return call_api("/query", method="POST", data=payload)

def get_example_queries() -> Dict:
    """Get categorized example queries from API"""
    try:
        response = call_api("/examples")
        return response
    except:
        return {"categories": []}

def detect_chart_type(df: pd.DataFrame) -> Optional[str]:
    """
    Detect appropriate chart type based on dataframe structure

    Args:
        df: Pandas DataFrame with query results

    Returns:
        Chart type string or None if no chart recommended
    """
    if df.empty or len(df.columns) < 2:
        return None

    # Get column info
    num_cols = df.select_dtypes(include=['number']).columns.tolist()
    cat_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
    date_cols = df.select_dtypes(include=['datetime64']).columns.tolist()

    # Time series: has date column + numeric values
    if date_cols and num_cols:
        return "line"

    # Check for month/year columns (common in aggregated data)
    time_keywords = ['month', 'year', 'date', 'day', 'week', 'quarter', 'year_month']
    has_time_col = any(keyword in col.lower() for col in df.columns for keyword in time_keywords)

    if has_time_col and num_cols:
        return "line"

    # Single categorical column + numeric values = bar chart
    if len(cat_cols) == 1 and num_cols:
        # If few rows, use bar chart
        if len(df) <= 20:
            return "bar"
        # If many rows, still bar but will be scrollable
        return "bar"

    # Multiple categorical columns + numeric = grouped bar
    if len(cat_cols) > 1 and num_cols:
        return "grouped_bar"

    # Two numeric columns = scatter plot
    if len(num_cols) >= 2 and len(df) > 3:
        return "scatter"

    # Single numeric column = histogram
    if len(num_cols) == 1 and len(df) > 5:
        return "histogram"

    return None

def create_chart(df: pd.DataFrame, chart_type: str) -> Optional[go.Figure]:
    """
    Create appropriate chart based on detected type

    Args:
        df: Pandas DataFrame with query results
        chart_type: Type of chart to create

    Returns:
        Plotly figure or None
    """
    if df.empty:
        return None

    try:
        # Get numeric and categorical columns
        num_cols = df.select_dtypes(include=['number']).columns.tolist()
        cat_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        date_cols = df.select_dtypes(include=['datetime64']).columns.tolist()

        # Detect time-like columns
        time_keywords = ['month', 'year', 'date', 'day', 'week', 'quarter', 'year_month']
        time_cols = [col for col in df.columns if any(keyword in col.lower() for keyword in time_keywords)]

        if chart_type == "line":
            # Time series line chart
            x_col = date_cols[0] if date_cols else (time_cols[0] if time_cols else cat_cols[0])

            # If multiple numeric columns, create multi-line chart
            if len(num_cols) > 1:
                fig = go.Figure()
                for y_col in num_cols[:5]:  # Limit to 5 lines max
                    fig.add_trace(go.Scatter(
                        x=df[x_col],
                        y=df[y_col],
                        mode='lines+markers',
                        name=y_col.replace('_', ' ').title()
                    ))
                fig.update_layout(
                    title="Trend Analysis",
                    xaxis_title=x_col.replace('_', ' ').title(),
                    yaxis_title="Value",
                    hovermode='x unified'
                )
            else:
                y_col = num_cols[0]
                fig = px.line(
                    df,
                    x=x_col,
                    y=y_col,
                    markers=True,
                    title=f"{y_col.replace('_', ' ').title()} Over Time"
                )

            return fig

        elif chart_type == "bar":
            # Bar chart
            x_col = cat_cols[0] if cat_cols else df.columns[0]
            y_col = num_cols[0] if num_cols else df.columns[1]

            # Sort by value for better visualization
            df_sorted = df.sort_values(by=y_col, ascending=False)

            fig = px.bar(
                df_sorted,
                x=x_col,
                y=y_col,
                title=f"{y_col.replace('_', ' ').title()} by {x_col.replace('_', ' ').title()}",
                color=y_col,
                color_continuous_scale='Blues'
            )

            # Rotate x-axis labels if many categories
            if len(df) > 10:
                fig.update_xaxes(tickangle=-45)

            return fig

        elif chart_type == "grouped_bar":
            # Grouped bar chart
            x_col = cat_cols[0]
            color_col = cat_cols[1] if len(cat_cols) > 1 else None
            y_col = num_cols[0]

            fig = px.bar(
                df,
                x=x_col,
                y=y_col,
                color=color_col,
                barmode='group',
                title=f"{y_col.replace('_', ' ').title()} Comparison"
            )

            return fig

        elif chart_type == "scatter":
            # Scatter plot
            x_col = num_cols[0]
            y_col = num_cols[1]
            color_col = cat_cols[0] if cat_cols else None

            fig = px.scatter(
                df,
                x=x_col,
                y=y_col,
                color=color_col,
                title=f"{y_col.replace('_', ' ').title()} vs {x_col.replace('_', ' ').title()}",
                hover_data=df.columns.tolist()
            )

            return fig

        elif chart_type == "histogram":
            # Histogram
            col = num_cols[0]

            fig = px.histogram(
                df,
                x=col,
                title=f"Distribution of {col.replace('_', ' ').title()}",
                nbins=min(50, len(df) // 2)
            )

            return fig

        return None

    except Exception as e:
        st.error(f"Error creating chart: {str(e)}")
        return None

# ============================================================================
# UI Components
# ============================================================================

def render_sidebar():
    """Render sidebar with examples and info"""
    with st.sidebar:
        st.title("🏥 MedFlow Analytics")
        st.caption("AI-Powered Healthcare Claims Analysis")

        st.divider()

        # API Status
        st.subheader("📡 System Status")
        try:
            health = call_api("/health")
            if health.get("status") == "healthy":
                st.success("✅ API: Healthy")
            else:
                st.warning(f"⚠️  API: {health.get('status')}")

            # Component status (skip 'api' since we show overall status above)
            components = health.get("components", {})
            for component, status in components.items():
                if component == "api":
                    continue  # Skip API component to avoid duplication
                if status in ["healthy", "connected"]:
                    st.success(f"✅ {component.replace('_', ' ').title()}: {status.title()}")
                else:
                    st.warning(f"⚠️  {component.replace('_', ' ').title()}: {status.title()}")

        except:
            st.error("❌ API: Offline")

        st.divider()

        # Example Queries
        st.subheader("💡 Example Questions")
        st.caption("Click any question to run it")

        examples_data = get_example_queries()
        categories = examples_data.get("categories", [])

        if categories:
            for category in categories:
                category_name = category.get("category", "Examples")
                category_icon = category.get("icon", "📝")
                category_desc = category.get("description", "")
                category_examples = category.get("examples", [])

                # Create expander for each category
                with st.expander(f"{category_icon} {category_name}", expanded=False):
                    st.caption(category_desc)
                    st.divider()

                    for example in category_examples:
                        question = example.get("question", "")
                        complexity = example.get("complexity", "simple")

                        # Add badge for complexity
                        complexity_color = {
                            "simple": "🟢",
                            "medium": "🟡",
                            "complex": "🔴"
                        }.get(complexity, "⚪")

                        if st.button(
                            f"{complexity_color} {question}",
                            key=f"example_{example.get('id', question[:10])}",
                            use_container_width=True,
                            help=f"Complexity: {complexity}"
                        ):
                            st.session_state.example_clicked = question
                            st.rerun()
        else:
            st.info("No examples available")

        st.divider()

        # Information
        st.subheader("ℹ️ About")
        st.markdown("""
        This AI-powered interface allows you to query healthcare claims data using natural language.

        **Features:**
        - Natural language to SQL conversion
        - Real-time query execution
        - Healthcare terminology understanding
        - Data quality insights

        **Tech Stack:**
        - Frontend: Streamlit
        - Backend: FastAPI
        - LLM: Claude 3.5 Sonnet
        - Database: Apache Iceberg
        """)

        st.divider()

        # Clear Chat
        if st.button("🗑️ Clear Chat History", use_container_width=True):
            st.session_state.messages = []
            st.rerun()

def render_chat_interface():
    """Render main chat interface"""

    # Title
    st.title("🤖 Healthcare Claims AI Assistant")
    st.caption("Ask questions about claims data in plain English")

    st.divider()

    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            if message["role"] == "user":
                st.write(message["content"])
            else:
                # Assistant message with SQL and results
                render_assistant_message(message)

    # Chat input (always visible)
    prompt = st.chat_input("Ask a question about claims data...")

    # Check if example was clicked (takes priority over manual input)
    if "example_clicked" in st.session_state:
        prompt = st.session_state.example_clicked
        del st.session_state.example_clicked

    # Process new query
    if prompt:
        # Add user message
        st.session_state.messages.append({"role": "user", "content": prompt})

        # Display user message
        with st.chat_message("user"):
            st.write(prompt)

        # Execute query and display response
        with st.chat_message("assistant"):
            with st.spinner("🤔 Thinking..."):
                try:
                    response = execute_query(prompt)

                    # Store assistant response
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": response
                    })

                    # Render response
                    render_assistant_message({"content": response})

                except Exception as e:
                    st.error(f"Error: {str(e)}")

def render_assistant_message(message: Dict):
    """
    Render assistant message with SQL, results, and explanation

    Args:
        message: Message dictionary with content
    """
    response = message["content"]

    # Explanation
    st.info(response.get("explanation", ""))

    # SQL Query
    with st.expander("🔍 Generated SQL Query", expanded=False):
        st.code(response.get("sql", ""), language="sql")

    # Results
    data = response.get("data", [])
    row_count = response.get("row_count", 0)

    if row_count > 0:
        st.success(f"✅ Found {row_count} result{'s' if row_count != 1 else ''}")

        # Create dataframe (keep raw for charting)
        df_raw = pd.DataFrame(data)

        # Detect and create chart
        chart_type = detect_chart_type(df_raw)

        if chart_type:
            # Show chart
            fig = create_chart(df_raw, chart_type)
            if fig:
                st.plotly_chart(fig, use_container_width=True)

                # Add toggle to show/hide table
                show_table = st.checkbox("📊 Show Data Table", value=True, key=f"table_{id(message)}")
            else:
                show_table = True
        else:
            show_table = True

        # Display table if requested or no chart
        if show_table:
            # Create formatted dataframe for display
            df_display = df_raw.copy()

            # Format numeric columns
            for col in df_display.columns:
                if df_display[col].dtype in ['float64', 'float32']:
                    # Check if it's currency (amount in name)
                    if 'amount' in col.lower():
                        df_display[col] = df_display[col].apply(lambda x: f"${x:,.2f}")
                    else:
                        df_display[col] = df_display[col].apply(lambda x: f"{x:,.2f}")

            st.dataframe(df_display, use_container_width=True)

        # Download button
        csv = df_raw.to_csv(index=False)
        st.download_button(
            label="📥 Download Results (CSV)",
            data=csv,
            file_name=f"query_results_{response.get('session_id', 'export')}.csv",
            mime="text/csv",
            key=f"download_{id(message)}"
        )

    else:
        st.warning("No results found")

    # Query metadata
    with st.expander("📊 Query Metadata"):
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Execution Time", f"{response.get('execution_time_ms', 0):.2f} ms")
        with col2:
            st.metric("Rows Returned", row_count)

# ============================================================================
# Main Application
# ============================================================================

def main():
    """Main application entry point"""

    # Render sidebar
    render_sidebar()

    # Render main chat interface
    render_chat_interface()

if __name__ == "__main__":
    main()
