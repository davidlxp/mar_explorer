# Tradeweb MAR Explorer

An AI-powered exploration tool for Tradeweb's Monthly Activity Report (MAR) data, focusing on accuracy and data-driven insights.

## Table of Contents

- [Overview](#overview)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Environment Configuration](#environment-configuration)
  - [Automated Data Migration](#automated-data-migration)
- [Run the Project](#run-the-project)
- [Tech Stack](#tech-stack)
- [AI Workflow Architecture](#ai-workflow-architecture)
- [Recommended Future Improvement](#recommended-future-improvement)

## Overview

Tradeweb MAR Explorer is a specialized tool to provide interactive analysis and natural language querying for Tradeweb's Monthly Activity Report (MAR) data and Press Release (PR). The system prioritizes accuracy and data fidelity while offering an intuitive interface for data exploration.

## Getting Started

### Prerequisites

- Python 3.11
- Virtual environment capability
- Git

### Installation

1. Create and activate virtual environment:

   ```bash
   python3.11 -m venv .venv
   source .venv/bin/activate
   ```

2. Install dependencies:

   ```bash
   pip install --upgrade pip setuptools wheel
   pip install -r requirements.txt
   ```

3. Install required components:

   ```bash
   playwright install chromium
   python -m spacy download en_core_web_sm
   ```

   Note: If the `spacy download` fails, you can install directly from the release:

   ```bash
   pip install https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.7.1/en_core_web_sm-3.7.1.tar.gz
   ```

### Environment Configuration

Create a `.env` file in the root directory with the following variables:

```env
# OpenAI Configuration
OPENAI_API_KEY=your_openai_api_key

# Pinecone Configuration
PINECONE_API_KEY=your_pinecone_api_key
PINECONE_ENV=your_pinecone_environment
INDEX_NAME=pr-index
PINECONE_NAMESPACE=tradeweb-namespace

# Snowflake Configuration
SNOWFLAKE_USER=your_snowflake_username
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOEFLAKE_ACCOUNT=your_snowflake_account
SNOWFLAKE_WAREHOUSE=your_snowflake_warehouse
```

Make sure to replace all placeholder values (e.g., `your_openai_api_key`) with your actual API keys and configuration values.

**Note**: Never commit the `.env` file to version control. The repository's `.gitignore` should include `.env` to prevent accidental commits of sensitive information.

### Automated Data Migration

**Skip the below step if the table in Snowflake and Pinecone have both setup**

1. For Snowflake, you need to create database & tables first - if they are not already there:

- Add below commands to `main.py` file

```
from services.db import get_database
db = get_database()
db._run_migrations()
```

- Then run the command below in your terminal

```
python main.py
```

- Lastly, calling the module below to automatically crawl the latest MAR file and ingest into Snowflake table

```
from services import task_handle_mar
task_handle_mar.crawl_latest_mar_file()
```

2. For Pinecone vectorDB, please making sure you have done below:

- created an index named `INDEX_NAME=pr-index` in Pinecone (`Index` is a container there for holding embedded text chunks)

- Add code below to `main.py` file for chunking and upload the Press Release (PR) data into Pinecone DB

```
import services.task_handle_pr as task_pr
task_pr.ingest_all_pr_md_in_storage()
```

- Then run the command below in your terminal

```
python main.py
```

## Run The Project

Once you have completed all the installation steps and configured your `.env` file, you can run the project using:

```bash
streamlit run app/app.py
```

The application will be available at `http://localhost:8501` in your web browser.

## Tech Stack

### Frontend

- **Streamlit**: Web-based user interface framework

### Backend

- **Python**: Core programming language
  (The code under services/ are modulized, and is ready to be migrated to backend framework like FAST API)

### Database

- **Snowflake**: Cloud data warehouse for MAR data

### Web Crawling

- **Crawl4ai & Playwright**: Automated web crawling

### Text Embedding (For AI Workflow)

- **Spacy**: Provides model to analyze and chunk texts intelligently before doing vectorization (e.g., Do not split sentence in the middle)
- **Pinecone**: Vector database for semantic search

### Building AI Agent

- **OpenAI**: LLM for building AI workflow

## AI Workflow Architecture

The AI workflow in this project follows a sophisticated multi-agent architecture designed for accurate and reliable query processing. Below is a detailed diagram of the workflow:

### Agent Roles

1. **Receptionist**
   (At: services/ai_workflow/agents/receptionist.py)

   - First point of contact for user queries
   - Determines if query needs clarification
   - Ensures query is within system scope

2. **Query Breaker**
   (At: services/ai_workflow/agents/query_breaker.py)

   - Breaks down complex queries into the next atomic, manageable tasks
   - Considers prior task context
   - Ensures logical task sequencing

3. **Task Planner**
   (At: services/ai_workflow/agents/task_planner.py)

   - Creates execution plans for one atomic task
   - Determines appropriate actions and tools
   - Provides context for execution

4. **Task Executor**
   (At: services/ai_workflow/utils/executor_logic.py)

   - Implements planned actions
   - Handles SQL queries, data retrieval, calculation and analysis
   - Returns structured results
   - Please note: This is actually a program, not agent

5. **Validator**
   (At: services/ai_workflow/agents/validator.py)

   - Validates task results against the requirement of current task
   - Ensures high confidence in outputs
   - Triggers task retry if confidence is low

6. **Aggregator**
   (At: services/ai_workflow/agents/aggregator.py)
   - Combines results from multiple tasks
   - Generates coherent final responses
   - Ensures proper citation and confidence scoring

### AI Workflow Process

1. User submits a query which is first evaluated by the Receptionist
2. If query is clear, it enters the Task Processing Loop
3. Query Breaker identifies next task to accomplish
4. Task Planner creates detailed execution plan
5. Executor performs the task and returns results
6. Validator ensures quality (loops back if confidence is low)
7. Process repeats until all tasks are complete
8. Aggregator combines results into final answer

### Benefit of this architecture

- Systematic query processing
- High accuracy through validation
- Graceful handling of complex queries
- Clear audit trail of decisions
- Robust error handling

## Recommended Future Improvement

### Frontend

- Enabling stacked bar to show the split of volume over different levels (e.g., “Asset Class”, “Product Type”, and “Product”). Providing toggle for users to switch between levels

- Addon to the feature above, maybe when the user click on a specific bar, we can zoom-into it. In the zoom-in view, the user can see the breakdown % of each different items in the level of their choice (in terms of the volume). It provides a deep dive view.

- On the dashboard, being able to highlight the _Highest_ and _Lowest_ volume using 2 colors

- Ensure that when a asset_class or category_type is removed, the corresponding product will be removed too. Test and validate against the MAR excel file to be sure. If it's out of sync, fix the frontend logic

- Enable to show the user what process the AI agent is working on right now

- Fix the weird text formatting issue that we sometimes see in the frontend

- The MoM & YoY shown when hovering over the bar can go wrong sometimes after we change the filter fields. I assume when cursor writing this code, the MoM and YoY is not recalculated properly - it may summed up the MoM & YoY when the baseline data refreshed rather than recalculate completely

### Services

- Built a crawling tool to automatically scan TradeWeb's website and collect the Press Release report URL. Having a config specify "Scanning X pages to fetch reports". So, we can automatically fetch the latest Press Release and add it to the Vector Database.

- There's a potential small issue with the services/chunk_utils.py when handling the upload process of some PR_FILES (e.g., `tradeweb_reports-monthly-2025_06.md`). The program will hang there and it's maybe due to an infinite-loop. Need to double check and fix this.

### AI Workflow

- Explicitly ask `task_planner` agent to do sorting when a SQL task involves finding the largest, smallest etc... So, when numerious data are pulled, the agent won't be overwhelmed and lead to wrong decision.

- Consider to add a new agent `execution_summarizer` dedicate for interpreting the SQL action result. So the it will be an easier load for the aggregator to consume data and generate the final result. It can improve the accuracy

- Adding `WEB_SEARCH` as a new `TodoIntent` item in `services\ai_workflow\data_model.py`. Then, enable the `task_planner` to choose `WEB_SEARCH`. Finally, add `WEB_SEARCH` as a new action item for `services\ai_workflow\utils\executor_logic.py`. For execution, we can leverage the `Perplexity API`, which is an AI agent API that can do web search, filtering out irrelevant info and provide grounded result with references.

- POTENTIAL SPEED IMPROVMENT: Consider architecting a system that generates multiple tasks at a time. This list of tasks should be able to be done in-parallel (No dependency between them). After running them in parallel, we can pass all the results back to the `query_breaker` to see if further tasks are needed. If no more task is needed, the existing completed tasks can be sent to `aggregator` for creating the final output. If more tasks are needed, we can do another set of batch tasks execution. In short, enabling processing tasks in-parallel can improve the agents response speed.
