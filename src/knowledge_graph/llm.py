"""LLM interaction utilities for knowledge graph generation."""
import requests
import json
import re

def call_llm(model, user_prompt, api_key, system_prompt=None, max_tokens=1000, temperature=0.2, base_url=None, timeout=300) -> str:
    """
    Call the language model API.

    Args:
        model: The model name to use
        user_prompt: The user prompt to send
        api_key: The API key for authentication
        system_prompt: Optional system prompt to set context
        max_tokens: Maximum number of tokens to generate
        temperature: Sampling temperature
        base_url: The base URL for the API endpoint
        timeout: Request timeout in seconds

    Returns:
        The model's response as a string
    """
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {api_key}"
    }

    messages = []

    # Merge system prompt into user prompt for compatibility with local LLMs
    # that don't support the system role (like Mistral in LM Studio)
    if system_prompt:
        combined_prompt = f"{system_prompt}\n\n{user_prompt}"
    else:
        combined_prompt = user_prompt

    messages.append({
        'role': 'user',
        'content': combined_prompt
    })

    payload = {
        'model': model,
        'messages': messages,
        'max_tokens': max_tokens,
        'temperature': temperature
    }

    response = requests.post(
        base_url,
        headers=headers,
        json=payload,
        timeout=timeout
    )
    
    if response.status_code == 200:
        return response.json()['choices'][0]['message']['content']
    else:
        raise Exception(f"API request failed: {response.text}")

def _repair_json_string(json_str):
    """
    Apply progressive repairs to a JSON string.

    Handles common LLM output issues:
    - Missing commas between objects: }{ -> },{
    - Missing commas between properties: "value""key" -> "value","key"
    - Control characters in strings
    - Trailing commas before ] or }
    - Unquoted property keys

    Returns:
        Parsed JSON object if repair succeeded, None otherwise.
    """
    # Strip control characters (except newline, tab, carriage return)
    cleaned = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', json_str)

    # Try parsing as-is after control char strip
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Insert missing commas between objects: }{ or }\n{ or } {
    fixed = re.sub(r'\}\s*\{', '},{', cleaned)

    # Insert missing commas between properties: "value" "key" or "value""key"
    # Matches: closing quote, optional whitespace, opening quote followed by a key pattern
    fixed = re.sub(r'"\s*"(\w+)"\s*:', r'","\1":', fixed)

    # Insert missing commas between value and next key: "value" "key":
    # This catches: "some value"  "next_key":
    fixed = re.sub(r'("(?:[^"\\]|\\.)*")\s+("(?:[^"\\]|\\.)*"\s*:)', r'\1,\2', fixed)

    # Fix trailing commas before ] or }
    fixed = re.sub(r',(\s*[\]}])', r'\1', fixed)

    try:
        return json.loads(fixed)
    except json.JSONDecodeError:
        pass

    # Try fixing unquoted property keys
    fixed2 = re.sub(r'(?<=[\{,])\s*(\w+)\s*:', r' "\1":', fixed)
    # Fix trailing commas again after key quoting
    fixed2 = re.sub(r',(\s*[\]}])', r'\1', fixed2)

    try:
        return json.loads(fixed2)
    except json.JSONDecodeError:
        pass

    return None


def _extract_complete_objects(text, start_idx):
    """
    Extract all complete JSON objects from a text starting at start_idx.
    Uses brace counting to find matched { } pairs.

    Returns:
        List of raw JSON object strings.
    """
    objects = []
    obj_start = -1
    brace_count = 0
    in_string = False
    escape_next = False

    for i in range(start_idx, len(text)):
        ch = text[i]

        if escape_next:
            escape_next = False
            continue
        if ch == '\\' and in_string:
            escape_next = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue

        if ch == '{':
            if brace_count == 0:
                obj_start = i
            brace_count += 1
        elif ch == '}':
            brace_count -= 1
            if brace_count == 0 and obj_start != -1:
                objects.append(text[obj_start:i + 1])
                obj_start = -1

    return objects


def extract_json_from_text(text):
    """
    Extract JSON array from text that might contain additional content.

    Args:
        text: Text that may contain JSON

    Returns:
        The parsed JSON if found, None otherwise
    """
    # First, check if the text is wrapped in code blocks with triple backticks
    code_block_pattern = r'```(?:json)?\s*([\s\S]*?)```'
    code_match = re.search(code_block_pattern, text)
    if code_match:
        text = code_match.group(1).strip()
        print("Found JSON in code block, extracting content...")

    # Try direct parsing in case the response is already clean JSON
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Look for opening bracket of a JSON array
    start_idx = text.find('[')
    if start_idx == -1:
        print("No JSON array start found in text")
        return None

    # Bracket counting to find matching closing bracket
    bracket_count = 0
    complete_json = False
    json_str = ""
    for i in range(start_idx, len(text)):
        if text[i] == '[':
            bracket_count += 1
        elif text[i] == ']':
            bracket_count -= 1
            if bracket_count == 0:
                json_str = text[start_idx:i + 1]
                complete_json = True
                break

    # Strategy 1: Try repairing the bracketed JSON string
    if complete_json:
        # Check if there are more arrays after this one (Extra data case)
        remainder = text[start_idx + len(json_str):].strip()
        if remainder.startswith('['):
            # Multiple arrays — fall through to Strategy 3
            print("Multiple arrays detected, merging...")
        else:
            result = _repair_json_string(json_str)
            if result:
                return result
            print("Found JSON-like structure but repair failed. Trying object extraction...")
    else:
        print("Found incomplete JSON array, attempting to extract complete objects...")

    # Strategy 2: Extract individual complete objects and reassemble
    search_start = start_idx + 1 if start_idx != -1 else 0
    objects = _extract_complete_objects(text, search_start)

    if objects:
        reconstructed = "[\n" + ",\n".join(objects) + "\n]"
        result = _repair_json_string(reconstructed)
        if result:
            print(f"Recovered {len(objects)} objects via reconstruction")
            return result

    # Strategy 3: Handle "Extra data" — multiple arrays concatenated
    # e.g. [...][...] or [...]\n[...]
    all_arrays = re.findall(r'\[[\s\S]*?\]', text)
    if len(all_arrays) > 1:
        merged_objects = []
        for arr_str in all_arrays:
            parsed = _repair_json_string(arr_str)
            if parsed and isinstance(parsed, list):
                merged_objects.extend(parsed)
        if merged_objects:
            print(f"Merged {len(all_arrays)} arrays into {len(merged_objects)} objects")
            return merged_objects

    print("No complete JSON array could be extracted")
    return None