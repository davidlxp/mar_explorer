from services.ai_workflow.data_model import (
    TodoIntent, PlanningResult, ExecutionOutput
)
from services.ai_workflow.utils.common_utils import (
    execute_sql_query,
    execute_vector_query,
    parse_pinecone_response
)
import logging
from services.ai_workflow.data_model import CalculatorResult
import services.task_handle_mar as task_handle_mar
from simpleeval import simple_eval

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def execute_task(plan: PlanningResult) -> ExecutionOutput:
    try:
        if plan.todo_intent == TodoIntent.NUMERIC:
            if not plan.helper_for_action:
                return ExecutionOutput("numeric", None, None, error="Missing SQL query")

            try:
              result = execute_sql_query(plan.helper_for_action)  # [[col1, col2...], [val1, val2...]]
              
              # Get the reference
              url_ref = task_handle_mar.get_latest_mar_meta_from_storage()['url']
              sql_ref = plan.helper_for_action
              reference = f"URL_REF: {url_ref} | SQL_REF: {sql_ref}"
      
              return ExecutionOutput(intent="numeric", content=result, raw_query=plan.helper_for_action, reference=reference)
            except Exception as e:
                logger.error(f"Error executing numeric task: {e}", exc_info=True)
                return None

        elif plan.todo_intent == TodoIntent.CONTEXT:
            if not plan.helper_for_action:
                return ExecutionOutput("context", None, None, error="Missing vector query")

            try:
              raw = execute_vector_query(plan.helper_for_action)
              parsed = parse_pinecone_response(raw)  # RetrievalResult

              # Get the reference
              url_ref = parsed.chunks[0].url
              report_name_ref = parsed.chunks[0].report_name
              text_ref = parsed.chunks[0].text
              reference = f"URL_REF: {url_ref} | REPORT_NAME_REF: {report_name_ref} | TEXT_REF: {text_ref}"

              return ExecutionOutput(intent="context", content=parsed, raw_query=plan.helper_for_action, reference=reference)
            except Exception as e:
                logger.error(f"Error executing context task: {e}", exc_info=True)
                return None

        elif plan.todo_intent == TodoIntent.CALCULATION:
            if not plan.helper_for_action:
                return ExecutionOutput(intent="calculation", content=None, raw_query=None, reference=None, error="Missing math expression")

            try:
                result = simple_eval(plan.helper_for_action)
                return ExecutionOutput(intent="calculation", content=CalculatorResult(result), raw_query=plan.helper_for_action, reference="CALCULATED_FROM_TASKS")
            except Exception as e:
                logger.error(f"Error executing calculation task: {e}", exc_info=True)
                return None

        elif plan.todo_intent == TodoIntent.AGGREGATION:
            return ExecutionOutput(intent="aggregation", content={"aggregation_result": "Task requires aggregation"}, raw_query=None, reference=None)

        logger.error(f"Unknown intent: {plan.todo_intent}", exc_info=True)
        return None

    except Exception as e:
        logger.error(f"Error executing task: {e}", exc_info=True)
        return None