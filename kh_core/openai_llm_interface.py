import httpx
import os
import json
from typing import Any

class OpenAILLMInterface:
    """
    Interface for calling OpenAI GPT models asynchronously to extract structured knowledge from plain text.
    """
    def __init__(self, api_key: str = None, model: str = "gpt-5-mini"):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OpenAI API key must be provided via argument or OPENAI_API_KEY env var.")
        
        self.model = model
        self.base_url = "https://api.openai.com/v1"
    
    async def chat_completion(self, messages: list, model: str = None, response_format: dict = None) -> str:
        """
        Make an async HTTP call to OpenAI's chat completion API. More efficient - used in processs_text.py
        
        Args:
            messages: List of message dictionaries with 'role' and 'content'
            model: Model to use (defaults to self.model)
            response_format: Optional response format specification (e.g., for JSON schema)
            
        Returns:
            The content of the assistant's response
        """
        model = model or self.model
        
        # Prepare the request payload
        payload = {
            "model": model,
            "messages": messages
        }
        
        # Add response_format if provided
        if response_format:
            payload["response_format"] = response_format
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=60.0
            )
            
            if response.status_code == 200:
                result = response.json()
                return result["choices"][0]["message"]["content"]
            else:
                raise Exception(f"OpenAI API error: {response.status_code} - {response.text}")

    async def chat_completion_full(self, messages: list, model: str = None, response_format: dict = None, tools: list = None, tool_choice: Any = None) -> dict:
        """
        Chat completion that returns the full assistant message (including tool_calls) and supports tool inputs.
        """
        model = model or self.model
        payload = {
            "model": model,
            "messages": messages
        }
        if response_format:
            payload["response_format"] = response_format
        if tools:
            payload["tools"] = tools
        if tool_choice is not None:
            payload["tool_choice"] = tool_choice

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=60.0
            )
            if response.status_code == 200:
                result = response.json()
                return result["choices"][0]["message"]
            else:
                raise Exception(f"OpenAI API error: {response.status_code} - {response.text}")
