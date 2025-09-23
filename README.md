# Tradeweb MAR Explorer

An AI-powered exploration tool for Tradeweb's Monthly Activity Report (MAR) data, focusing on accuracy and data-driven insights.

## Overview

Tradeweb MAR Explorer is a specialized tool designed to provide interactive analysis and natural language querying capabilities for Tradeweb's Monthly Activity Report data. The system prioritizes accuracy and data fidelity while offering an intuitive interface for data exploration.

## Approach

Our approach focuses on several key principles:

1. **Data Integrity**

   - Strict adherence to source data boundaries
   - Primary source: MAR Excel file (ADV-M and Volume-M tabs)
   - Secondary source: Associated press releases for context
   - No external data sources are used

2. **Architecture**

   - Web-based interface for accessibility
   - Modular design for maintainability
   - Structured data persistence for historical tracking
   - Natural Language Processing (NLP) for query understanding

3. **Quality Assurance**
   - Comprehensive logging of system interactions
   - Tracking of unanswered or low-confidence questions
   - Regular data validation checks

## Data Handling

The system implements a robust data processing pipeline:

1. **Data Ingestion**

   - Automated ingestion of MAR Excel files
   - Focus on ADV-M and Volume-M tabs
   - Press release parsing for contextual information
   - Data validation and cleaning

2. **Data Storage**

   - Monthly snapshots for historical comparison
   - Structured storage for efficient querying
   - Version control of data changes
   - Backup and recovery mechanisms

3. **Data Access**
   - Optimized query patterns
   - Caching strategies for frequent requests
   - Rate limiting for stability

## Grounding

The system ensures all responses are grounded in source data:

1. **Source Attribution**

   - Every non-trivial fact is cited to source
   - Clear distinction between Excel and press release sources
   - Confidence scoring for responses

2. **Context Management**
   - Maintenance of query context
   - Historical context for trend analysis
   - User session context for personalized experience

## Accuracy Strategy

Accuracy is maintained through multiple layers:

1. **Data Verification**

   - Cross-validation between data sources
   - Automated consistency checks
   - Regular data audits

2. **Response Validation**

   - Multi-step verification process
   - Confidence threshold enforcement
   - Explicit uncertainty communication

3. **Monitoring and Improvement**
   - Tracking of accuracy metrics
   - User feedback collection
   - Continuous model refinement

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

   Note: If the spacy download fails, you can install directly from the release:

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

## License

[License information to be added]

## Contact

[Contact information to be added]
