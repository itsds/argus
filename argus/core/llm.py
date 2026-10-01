"""
LLM client factory for Argus.

Provides a single function to create the right LangChain chat model
based on config. Swap providers without touching agent code.
"""

from langchain_core.language_models import BaseChatModel

from argus.core.config import ArgusConfig


def create_llm(config: ArgusConfig) -> BaseChatModel:
    """
    Create a LangChain chat model from Argus config.

    Supported providers: google, openai, anthropic.
    """
    provider = config.llm.get("provider", "google")
    model = config.llm.get("model", "gemini-2.0-flash")
    temperature = config.llm.get("temperature", 0.0)

    if provider == "google":
        from langchain_google_genai import ChatGoogleGenerativeAI
        return ChatGoogleGenerativeAI(
            model=model,
            temperature=temperature,
        )

    elif provider == "openai":
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(
            model=model,
            temperature=temperature,
        )

    elif provider == "anthropic":
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(
            model=model,
            temperature=temperature,
        )

    else:
        raise ValueError(
            f"Unknown LLM provider: '{provider}'. "
            f"Supported: google, openai, anthropic"
        )
