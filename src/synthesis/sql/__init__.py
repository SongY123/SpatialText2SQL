"""Constraint-guided SQL synthesis package."""

from .config import (
    DEFAULT_SQL_SYNTHESIS_CONFIG_PATH,
    SQLExecutionCheckConfig,
    SQLSynthesisConfig,
    SQLSynthesisDBConfig,
    SQLSynthesisFunctionConfig,
    SQLSynthesisLLMConfig,
    SQLSynthesisLoggingConfig,
    SQLSynthesisRunConfig,
    load_sql_synthesis_config,
    override_sql_synthesis_config,
)
from .execution import SQLExecutionChecker
from .function_library import PostGISFunctionLibrary, infer_function_categories
from .generator import (
    MockSQLGenerator,
    OllamaSQLGenerator,
    OpenAICompatibleSQLGenerator,
    SQLGeneratorResponse,
    build_sql_generator,
)
from .io import (
    append_sql_queries,
    append_sql_query,
    ensure_sql_output,
    initialize_sql_output,
    load_input_databases,
    load_existing_sql_id_offsets,
    write_sql_queries,
)
from .models import (
    DIFFICULTY_LEVELS,
    PostGISFunction,
    SQLExecutionResult,
    SQLGenerationCandidate,
    SQLValidationResult,
    SynthesizedSQLQuery,
)
from .parser import parse_sql_generation_response
from .prompt_metadata import PostGISPromptMetadataProvider
from .schema_utils import build_create_table_ddl_query
from .synthesizer import ConstraintGuidedSQLSynthesizer
from .validator import SQLValidator, contains_dangerous_sql

__all__ = [
    "DEFAULT_SQL_SYNTHESIS_CONFIG_PATH",
    "DIFFICULTY_LEVELS",
    "ConstraintGuidedSQLSynthesizer",
    "MockSQLGenerator",
    "OllamaSQLGenerator",
    "OpenAICompatibleSQLGenerator",
    "PostGISFunction",
    "PostGISFunctionLibrary",
    "PostGISPromptMetadataProvider",
    "SQLExecutionChecker",
    "SQLExecutionCheckConfig",
    "SQLExecutionResult",
    "SQLGenerationCandidate",
    "SQLGeneratorResponse",
    "SQLSynthesisConfig",
    "SQLSynthesisDBConfig",
    "SQLSynthesisFunctionConfig",
    "SQLSynthesisLLMConfig",
    "SQLSynthesisLoggingConfig",
    "SQLSynthesisRunConfig",
    "SQLValidationResult",
    "SQLValidator",
    "SynthesizedSQLQuery",
    "contains_dangerous_sql",
    "build_sql_generator",
    "infer_function_categories",
    "append_sql_queries",
    "append_sql_query",
    "build_create_table_ddl_query",
    "ensure_sql_output",
    "initialize_sql_output",
    "load_input_databases",
    "load_existing_sql_id_offsets",
    "load_sql_synthesis_config",
    "override_sql_synthesis_config",
    "parse_sql_generation_response",
    "write_sql_queries",
]
