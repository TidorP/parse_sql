from cache_llm import JSONCacheAsync, RateLimiter
import json
from openai import OpenAI
import asyncio
import logging

cache = JSONCacheAsync()
rate_limiter =  RateLimiter()


openai_api_key = "open-ai-key"
client = OpenAI(api_key=openai_api_key)


# Define the database schema with tables and their respective columns
# In production, this will be an input to the system (depending on what we need to access)
DATABASE_SCHEMA = """
You are aware of the following table structures:

Table: orders
Columns:
    - order_id
    - status
    - gender
    - num_of_item
    - created_at

Table: order_items
Columns:
    - order_id
    - sale_price
"""

# utils function
def extract_between_braces(s):
    # Find the index of the first occurrence of "{"
    start_index = s.find("{")
    
    # Find the index of the last occurrence of "}"
    end_index = s.rfind("}")
    
    # Check if both braces are found in the string
    if start_index == -1 or end_index == -1 or start_index >= end_index:
        return "a{"  # Return bad string if no valid braces are found or they are misplaced
    
    # Extract and return the substring between the braces
    # print("Extracted: ", "{" +s[start_index + 1:end_index] + "}")
    return "{" +s[start_index + 1:end_index] + "}"

def call_llm(
    system_instructions,
    user_instructions,
    model="gpt-4o"
):
    try:
        if "gpt" in model:
            print(f"Attempting GPT call with model: {model}")
            logging.info(f"Attempting GPT call with model: {model}")
            completion = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": " ".join(system_instructions)},
                    {"role": "user", "content": " ".join(user_instructions)},
                ],
            )
            result_json = completion.choices[0].message.content
            result_json = json.loads(extract_between_braces(result_json))
            return 200, result_json
    except Exception as e:
        print("Error on call llm: ", e)
        logging.info(f"Error on call llm: {e}")
        return 500,  None

    return 500,  None


# V1 of the prompt
# can be improved by giving actual examples (a few from notion)
# however, even without examples, seems to be doing very well
# (e.g. even without few-shot, it puts joins when relevant and final jsons look very close to the given ones in Notion)
def prepare_prompt_query_generation(natural_query: str):
    """
    Prepares system and user instructions for an LLM to convert a natural language query
    into a JSON object containing 'query_json' and 'semantic_layer_json'.

    Parameters:
    - natural_query (str): The natural language query provided by the user.

    Returns:
    - tuple: A tuple containing two elements:
        1. system_instructions (list of str): Instructions that define the LLM's role and knowledge.
        2. user_instructions (list of str): Instructions that provide the user's query and desired output format.
    """

    # System instructions to set the context and provide necessary knowledge
    system_instructions = [
        "You are an AI assistant specialized in converting natural language queries into structured JSON formats suitable for database querying.",
        "You have access to the following database schema:",
        DATABASE_SCHEMA,
        "Your task is to interpret the user's natural language query and generate a JSON object that includes both 'query_json' and 'semantic_layer_json'.",
        "Ensure that all metrics, dimensions, and filters are accurately identified based on the query and correctly mapped to the database schema.",
        "If the query involves multiple tables, include the necessary join conditions in the 'semantic_layer_json'.",
        "The output JSON should adhere strictly to the specified structure without additional explanations or text."
    ]


    # Define the JSON schema as a Python dictionary
    json_schema = {
        "query_json": {
            "metrics": "List of metrics to be calculated (e.g., ['total_revenue'])",
            "dimensions": "List of dimensions to group by (e.g., ['status'])",
            "filters": [
                {
                    "field": "Field to filter on (e.g., 'status')",
                    "operator": "Operator for filtering (e.g., '=', '>', '<')",
                    "value": "Value for the filter (e.g., 'Complete', 1000)"
                }
            ]
        },
        "semantic_layer_json": {
            "metrics": [
                {
                    "name": "Name of the metric (e.g., 'total_revenue')",
                    "sql": "SQL expression for the metric (e.g., 'SUM(sale_price)')",
                    "table": "Table associated with the metric (e.g., 'order_items')"
                }
            ],
            "dimensions": [
                {
                    "name": "Name of the dimension (e.g., 'status')",
                    "sql": "SQL expression for the dimension (e.g., 'status')",
                    "table": "Table associated with the dimension (e.g., 'order_items')"
                }
            ],
            "joins": [
                {
                    "one": "Primary table in the join (e.g., 'orders')",
                    "many": "Secondary table in the join (e.g., 'order_items')",
                    "join": "Join condition (e.g., 'order_items.order_id = orders.order_id')"
                }
            ]
        }
    }

    
    # User instructions containing the natural language query and desired output format
    user_instructions = [
        "Please convert the following natural language query into the specified JSON format:",
        f"\"{natural_query}\"",
        "",
        "The JSON should have the following structure:",
        "```json",
        json.dumps(json_schema, indent=2),
        "```",
        "",
        "Ensure that:",
        "- All metrics are listed under the 'metrics' key in 'query_json'.",
        "- All dimensions are listed under the 'dimensions' key in 'query_json'.",
        "- All filters are listed under the 'filters' key in 'query_json' with their respective fields, operators, and values.",
        "- The 'semantic_layer_json' accurately defines each metric and dimension with their corresponding SQL expressions and associated tables.",
        "- If joins between tables are necessary, include them under a 'joins' key within 'semantic_layer_json'.",
        "- The output can be parsed by Python's json.loads function."
    ]

    return system_instructions, user_instructions


async def run_generate_query(question, model):
    system_instructions, user_instructions = prepare_prompt_query_generation(question)

    print(f"Calling LLM with model: {model}")
    logging.info(f"Calling LLM with model: {model}")

    # run through LLM
    status_code, output = call_llm(
        system_instructions,
        user_instructions,
        model=model
    )

    print(f"LLM call completed. Status: {status_code}")
    logging.info(f"LLM call completed. Status: {status_code}")

    return status_code, output


async def generate(question_id, question, model):
    print(f"Initialization")
    logging.info(f"Initialization.")
    models_to_try = ["gpt-4o","llama3-70b-8192", "o1-mini"]
    for model_candidate in models_to_try:
        cache_key = f"{model_candidate}__{question}"
        # Check the prompt cache first
        cached_result = await cache.get(cache_key)
        if cached_result:
            break

    if cached_result:
        output = cached_result
        status_code = 200
        print(f"Hit cache")
        logging.info(f"Hit cache. {cached_result}")
    else:
        print(f"No Hit cache")
        logging.info(f"No Hit cache.")
        status_code, output = await rate_limiter.call(run_generate_query, question, model)
        # Save the result to cache
        if status_code == 200:
            await cache.set(f"{model}__{question}", output)

    if status_code == 200:
        return 200, output
    else:
        return 500, ""
    

def main(question_id, question, model):
    asyncio.run(generate(question_id, question, model))

main(question_id=1, question="Show me order details for orders over $1000 in value", model="gpt-4o")